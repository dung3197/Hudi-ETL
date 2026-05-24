import sys,os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.dags.airflow_hooks import service_key

from datetime import datetime
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator

from src.configs.settings import settings
from src.helpers.dag_utils import get_start_time, datetime_from_str, str_from_datetime, table_names_collector
from src.helpers.omd_utils import load_inlets_outlets

from src.scripts.live.bo_schema_etl import BORawProcess
from src.scripts.live.bo_table_etl_base import BOETLBase

from src.helpers.metric_utils import MetricUtils
from src.helpers.exceptions import InputValidationError

from src.scripts.hash.cust_info_hash import cust_infor_warehouse_hash
from src.scripts.hash.account_info_hash import account_infor_warehouse_hash
from src.scripts.hash.mkt_info_hash import mkt_infor_warehouse_hash

from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.table import Table

import json

dag_id = "bo_etl_raw_to_warehouse"

silver_omd_service_name = "trino"
silver_omd_database_name = "hudi"
linage_key =  "process_BO_silver"

inlets_input_list = [{'entity': Container, 'fqn': f"MinIO.{settings.s3_bucket_name}.{settings.s3_bo_source_path}", 'key': linage_key}]
outlets_input_list = [{'entity': Table, 'fqn': f"{silver_omd_service_name}.{silver_omd_database_name}.{settings.hive_sync_database}.{table_name}", 'key': linage_key} for table_name in table_names_collector(BOETLBase)]


default_args = {
    'owner': f"{service_key}"
}


dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    start_date=datetime(2025, 2, 15, tzinfo=settings.timezone),
    schedule_interval="0 * * * *",
    # schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=["bo", "cdc", "ogg", "warehouse", f"{service_key}"]
)

def get_current_time(ti):
    curr_time = str_from_datetime(datetime.now(tz=settings.timezone))
    ti.xcom_push(key="curr_time", value=curr_time)

def get_last_process(ti):
    common_last_process_str = Variable.get("BO_COMMON_LAST_PROCESS", default_var=None)

    last_fail_process_dict_str = Variable.get("BO_LAST_FAIL_PROCESSORS", default_var=None)

    common_last_process = get_start_time(common_last_process_str)

    ti.xcom_push(key="common_last_process", value=str_from_datetime(common_last_process))

    ti.xcom_push(key="last_fail_process_dict", value=last_fail_process_dict_str)
    
def set_last_process(ti):
    fail_processor_dict = ti.xcom_pull(key="fail_processor_dict")
    curr_time = ti.xcom_pull(key="curr_time")
    
    Variable.set("BO_LAST_FAIL_PROCESSORS", fail_processor_dict)
    Variable.set("BO_COMMON_LAST_PROCESS", curr_time)

def etl(ti, **context):
    from_time_str = ti.xcom_pull(key="common_last_process")

    last_fail_process_dict_str = ti.xcom_pull(key="last_fail_process_dict")

    last_fail_dict = None

    if last_fail_process_dict_str != None:
        last_fail_process_dict_str = last_fail_process_dict_str.replace("'",'"')
        
        last_fail_dict = json.loads(last_fail_process_dict_str)

        for k, v in last_fail_dict.items():
            last_fail_dict[k] = datetime_from_str(v)
            if last_fail_dict[k] is None:
                raise InputValidationError(
                messages=f"BO_LAST_FAIL_PROCESSORS table {k} invalid value {v}"
            )
    

    from_time = datetime_from_str(from_time_str)
    curr_time = datetime_from_str(ti.xcom_pull(key="curr_time"))
    raw_process = BORawProcess()
    process_fail_status_dict = raw_process.etl_raw_to_hudi(from_time=from_time, to_time=curr_time, last_fail_dict=last_fail_dict) 

    """Process each item in return status list
    """

    ti.xcom_push(key="fail_processor_dict", value=process_fail_status_dict)

    context['task_instance'].metrics_data = MetricUtils().get_all_metrics()
    

def explicitity_set_final_dag_status():
    last_fail_process_list_str = Variable.get("BO_LAST_FAIL_PROCESSORS", default_var=None)
    last_fail_process_list_str = last_fail_process_list_str.replace("'",'"')
    last_fail_process_list = json.loads(last_fail_process_list_str)
    if last_fail_process_list != {}:
        print("NOT ALL TABLES ARE PROCESSED SUCCESSFULLY ! THEREFORE THROWING EXIT CODE 1")
        sys.exit(1)


get_last_process_task = PythonOperator(task_id='get_last_process', python_callable=get_last_process, dag=dag)
get_current_time_task = PythonOperator(task_id='get_current_time', python_callable=get_current_time, dag=dag)
etl_task = PythonOperator(task_id='etl', python_callable=etl, dag=dag, inlets=load_inlets_outlets(inlets_input_list), outlets=load_inlets_outlets(outlets_input_list))
set_last_process_task = PythonOperator(task_id='set_last_process', python_callable=set_last_process, dag=dag)
cust_infor_warehouse_hash_task = PythonOperator(task_id='cust_infor_warehouse_hash', python_callable=cust_infor_warehouse_hash, dag=dag)
account_infor_warehouse_hash_task = PythonOperator(task_id='account_infor_warehouse_hash', python_callable=account_infor_warehouse_hash, dag=dag)
mkt_info_warehouse_hash_task = PythonOperator(task_id='mkt_info_hash', python_callable=mkt_infor_warehouse_hash, dag=dag)
explicitity_set_final_dag_status_task = PythonOperator(task_id='explicitity_set_final_dag_status', python_callable=explicitity_set_final_dag_status, dag=dag)

get_last_process_task >> get_current_time_task >> etl_task >> set_last_process_task 
set_last_process_task >> [ cust_infor_warehouse_hash_task, mkt_info_warehouse_hash_task ] 
cust_infor_warehouse_hash_task >> account_infor_warehouse_hash_task 
account_infor_warehouse_hash_task >> explicitity_set_final_dag_status_task
mkt_info_warehouse_hash_task >> explicitity_set_final_dag_status_task

