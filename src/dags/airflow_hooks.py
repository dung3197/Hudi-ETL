import sys,os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from airflow.models import DAG
from airflow.models import Variable
from dotenv import load_dotenv
from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway
import logging
from tzlocal import get_localzone

from airflow.models.baseoperator import BaseOperator
import functools

from src.configs.settings import settings

load_dotenv()

server_tz = get_localzone()
dag_gauge:str = "Datalake_life_cycle"
gauge:str = "Gauge"


state_mapping = {"success": 1, "failed": 2, "": 0, "skipped": 3}


service_key = os.getenv("SERVICE_KEY")

pushgateway = os.getenv("PUSHGATEWAY_URL")

if not service_key or service_key.strip() == "":
    raise ValueError("Environment variable 'SERVICE_KEY' is required!")


def apply_variable_patch():
    # Store original method to Value
    original_get = Variable.get
    original_set = Variable.set

    # Override get function
    def custom_get(key, default_var=None, deserialize_json=False, **kwargs):
        return original_get(
            f"{service_key}_{key}", default_var, deserialize_json, **kwargs
        )

    # Override set function
    def custom_set(key, value, serialize_json=False, **kwargs):
        return original_set(f"{service_key}_{key}", value, serialize_json, **kwargs)

    # Apply patch
    Variable.get = custom_get
    Variable.set = custom_set


# Override DAG class with default callback
def apply_dag_callback():

    # Store DAG original constructor
    ori_init = DAG.__init__

    # Define Patch function replace the original constructor
    def patch_init(self, *args, **kwargs):
        # Check whether callbacks are defined in keyword args list or not
        if "on_success_callback" not in kwargs or kwargs["on_success_callback"] is None:
            kwargs["on_success_callback"] = lambda context: dag_callback(True, context)

        if "on_failure_callback" not in kwargs or kwargs["on_failure_callback"] is None:
            kwargs["on_failure_callback"] = lambda context: dag_callback(False, context)

        # Call original constructor with the same args and kwargs to ensure all init logic in orginal constructor remain the same
        ori_init(self, *args, **kwargs)

    # Replace the original constructor with Patch
    DAG.__init__ = patch_init


# Override Base Operator class with default callback

def apply_base_op_ti_callback():

    # Store DAG original constructor
    ori_init = BaseOperator.__init__

    # Define Patch function replace the original constructor
    @functools.wraps(ori_init)
    def patch_init(self, *args, **kwargs):
        

        # Check whether callbacks are defined in keyword args list or not
        if "on_success_callback" not in kwargs or kwargs["on_success_callback"] is None:
            kwargs["on_success_callback"] = lambda context: default_ti_scrapes(context)

        if "on_failure_callback" not in kwargs or kwargs["on_failure_callback"] is None:
            kwargs["on_failure_callback"] = lambda context: default_ti_scrapes(context)

        # Call original constructor with the same args and kwargs to ensure all init logic in orginal constructor remain the same
        return ori_init(self, *args, **kwargs)

    # Replace the original constructor with Patch
    BaseOperator.__init__ = patch_init


# PushGateway Pushing Method
def send(dag, state, start_time, end_time, list_ti_stats_values):
    try:
        registry = CollectorRegistry()

        Dag_life_cycle_gauge = Gauge(
            dag_gauge,
            "DAG & Task duration",
            ["Timestamps", "Dag"],
            registry=registry,
        )

        Dag_life_cycle_gauge.labels(
            **{
                "Dag": dag,
                "Timestamps": start_time
            }
        ).set(state_mapping[state])

        Dag_life_cycle_gauge.labels(
            **{ 
                "Dag": dag,
                "Timestamps": end_time
            }
        ).set(state_mapping[""])

        pushadd_to_gateway(pushgateway, job="airflow-central", grouping_key={"namespace": f"{dag}"}, registry=registry)

        for ti_stats in list_ti_stats_values: 
            try:
                ti_id = ti_stats.get("ti_id")
                ti_start_time = ti_stats.get("ti_start_date")
                ti_end_time = ti_stats.get("ti_end_date")
                ti_duration = ti_stats.get("ti_duration")
                ti_state = ti_stats.get("ti_state")

                ti_registry = CollectorRegistry()
                TI_life_cycle_gauge = Gauge(
                    dag_gauge,
                    "Task_duration",
                    ["Task", "Timestamps", "Dag"],
                    registry=ti_registry,
                )

                TI_life_cycle_gauge.labels(
                    **{
                        "Task": ti_id,
                        "Timestamps": ti_start_time,
                        "Dag": dag
                    }
                ).set(state_mapping[ti_state])

                TI_life_cycle_gauge.labels(
                    **{ 
                        "Task": ti_id, 
                        "Timestamps": ti_end_time,
                        "Dag": dag
                    }
                ).set(state_mapping[""])
                logging.info(f"TI statistics: {ti_stats}")
                pushadd_to_gateway(pushgateway, job=f"airflow_central", grouping_key={"namespace": f"{dag}.{ti_id}"}, registry=ti_registry)

            except Exception as SendTIMetricsException:
                print("When trying to send TI Metrics, the following exception occurs: ", SendTIMetricsException)

    except Exception as SendMetricsException:
        print("When trying to send Metrics, exception occurs: ", SendMetricsException)


def dag_callback(success: bool, context):

    dag_run = context.get("dag_run")
    dag_id = dag_run.dag_id

    start_date = dag_run.start_date
    start_date = start_date.astimezone(server_tz)
    start_time = start_date.strftime("%Y-%m-%d %H:%M:%S.%f")

    end_date = dag_run.end_date
    end_date = end_date.astimezone(server_tz)
    end_time = end_date.strftime("%Y-%m-%d %H:%M:%S.%f")

    duration = end_date - start_date

    state = dag_run.state
    
    print("STATE", state)

    task_instances = dag_run.get_task_instances()

    list_ti_stats_values = []

    for ti in task_instances:

        ti_id = ti.task_id

        ti_start_date = ti.start_date.astimezone(server_tz)
        ti_end_date = ti.end_date.astimezone(server_tz)

        ti_duration = ti.duration

        ti_status = ti.state

        ti_stats = {"ti_id": ti_id, "ti_start_date": ti_start_date, "ti_end_date": ti_end_date, "ti_duration":ti_duration, "ti_state": ti_status}
        list_ti_stats_values.append(ti_stats)

    print(f"TI list: {list_ti_stats_values}")
    send(dag_id, state, start_time, end_time, list_ti_stats_values)
    
    # Log the information
    print(f"DAG {dag_id} completed successfully.")
    print(f"Start Time: {start_time}")
    print(f"End Time: {end_time}")
    print(f"Duration: {duration}")
    print(f"Status: {state}")



def default_ti_scrapes(context): 

    if hasattr(context['task_instance'], "metrics_data") == True:
        print("ATTRIBUTE FOUND")
        metrics_data = context['task_instance'].metrics_data

        if gauge in metrics_data and len(metrics_data[gauge]) > 0 :
            
            grouped_by_key = {}
            
            for metrics_info in metrics_data[gauge]:
                namespace_key = metrics_info.get('grouping_key')
                if namespace_key not in grouped_by_key:
                    grouped_by_key[namespace_key] = []
                    
                grouped_by_key[namespace_key].append(metrics_info.get('metrics_data'))
                
            for namespace_key, metrics_datas in grouped_by_key.items():
                
                keys = []
                for metrics_data in metrics_datas:
                    metrics_data["svc_key"] = service_key
                    metrics_data["dag_id"] = context['task_instance'].dag_id
                    metrics_data["task_id"] = context['task_instance'].task_id
                    
                    keys += metrics_data.keys()
                keys = list(set(keys))
                
                # Tạo registry
                df_registry = CollectorRegistry()
                dataframe_process_gauge = Gauge(
                    f"Datalake_dataframe_metrics",
                    f"Datalake dataframe metrics",
                    keys,
                    registry = df_registry,
                )
                
                for metrics_data in metrics_datas:                    
                    dataframe_process_gauge.labels(**metrics_data)
                    
                if namespace_key is not None:
                    pushadd_to_gateway(pushgateway, job=f"airflow_central_df", grouping_key={"namespace": f"{namespace_key}"}, registry=df_registry)
                else:
                    pushadd_to_gateway(pushgateway, job=f"airflow_central_df", registry=df_registry)
    else:
        print("NOT FOUND ATTRIBUTE")
        pass


apply_variable_patch()
apply_dag_callback()
apply_base_op_ti_callback()

print("airflow_hooks loaded!")