import sys,os,json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.dags.airflow_hooks import service_key

from datetime import datetime
from airflow.models import DAG, Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from src.configs.settings import settings
from src.helpers.dag_utils import get_start_time, datetime_from_str, str_from_datetime, str_to_etl_processors
from src.scripts.live.bo_schema_etl import BORawProcess
from src.scripts.live.bo_table_etl_base import BOETLBase


dag_id = "bo_etl_raw_to_warehouse_force"
default_args = {
    'owner': f"{service_key}"
}
dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    start_date=datetime(2025, 2, 15, tzinfo=settings.timezone),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=["bo", "cdc", "force", "warehouse", f"{service_key}"],
    params={
        "force_tables": Param(
            default="",
            type="string",
            description="Danh sách các tables force đồng bộ lại từ đầu đến thời điểm chung"
        ),
        "from_time": Param(
            default="",
            type=["null", "string"],
            description="Thời điểm bắt đầu đồng bộ lại (Theo format YYYY-MM-DD HH:MM:SS)"
        )
    }
)

def get_last_process(ti):

    last_process_str = Variable.get("BO_COMMON_LAST_PROCESS", default_var=None)
    last_process = get_start_time(last_process_str)
    ti.xcom_push(key="last_process", value=str_from_datetime(last_process))
    
def etl(ti, **kwargs):

    last_fail_dict = None
    last_fail_process_str_dict = Variable.get("BO_LAST_FAIL_PROCESSORS", default_var=None) 

    if last_fail_process_str_dict != None: 
        last_fail_dict_str = last_fail_process_str_dict.replace("'",'"') 
        
        last_fail_dict = json.loads(last_fail_dict_str) 

        print('last_fail_list', last_fail_dict)
    
    to_time_str = ti.xcom_pull(key="last_process")
    to_time = datetime_from_str(to_time_str)
    
    tables = kwargs["params"]["force_tables"]
    from_time = kwargs["params"]["from_time"]

    if from_time != '':
        from_time = datetime_from_str(from_time)
    else:
        from_time = None

    print(f'Force start sync bo tables: {tables}')
    bo_processors = str_to_etl_processors(tables, BOETLBase)


    bo_process_fail_status_dict = {}
    if len(bo_processors) > 0:
        raw_process = BORawProcess(bo_processors)
        bo_process_fail_status_dict = raw_process.etl_raw_to_hudi(from_time=from_time, to_time=to_time)

    else:
        print(f"No table match: {tables}")

    if bo_process_fail_status_dict == {}:
        """Trường hợp không có processor nào chạy force bị fail => xóa key fail khỏi dict
        """
        for processor in bo_processors: 
            last_fail_dict.pop(processor.__name__, None)  

    else:
        """Trường hợp chạy force vẫn fail, processor nào success thì xóa khỏi variable, trường hợp nào fail thì giữ nguyên
        """ 
        fail_list = bo_process_fail_status_dict.keys()
        for key in list(last_fail_dict.keys()):
            if key not in list(fail_list) and key in list(tables.split(",")):
                del last_fail_dict[key]
                
    """Process each item in return status list
    """
    print('last_fail_list', last_fail_dict)
    Variable.set("BO_LAST_FAIL_PROCESSORS", last_fail_dict)  



get_last_process_task = PythonOperator(task_id='get_last_process', python_callable=get_last_process, dag=dag)
etl_task = PythonOperator(task_id='etl', python_callable=etl, dag=dag)

get_last_process_task >> etl_task 
