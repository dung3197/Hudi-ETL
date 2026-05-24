from datetime import datetime
from minio import Minio

from src.configs.secret_manager import get_s3_conn
from src.helpers.hudi_utils import create_session
from src.configs.settings import settings
from src.helpers.metric_utils import MetricUtils
from src.helpers.dag_utils import str_from_datetime


class SchemaETLBase:
    def __init__(self, app_name:str, processors=None):
        self.app_name = app_name
        self.processors = processors

    def etl_raw_to_hudi(self, from_time: datetime, to_time: datetime, last_fail_dict: dict = None):

        if last_fail_dict is None:
            last_fail_dict = {}

        if (from_time is not None and to_time is not None) and (from_time > to_time):
            print(f"Invalid time range {from_time} - {to_time}")
            return

        print(f"================START: OGG_{self.app_name}_ETL================")
        print(f"Process time range: {from_time} -> {to_time}")
        st = datetime.now(tz=settings.timezone)
        print(f"Start Time: {st}")

        s3_conn = get_s3_conn()
        client = Minio(
            s3_conn["s3_url"],
            access_key=s3_conn["s3_access_key"],
            secret_key=s3_conn["s3_secret_key"],
            secure=False,
        )
        spark_session = create_session(s3_conn)

        overwrite = False if from_time is not None else True

        try:
            for processor in self.processors:

                processor_from_time = from_time
                if processor.__name__ in last_fail_dict:
                    processor_from_time = last_fail_dict[processor.__name__]
                etl = processor(spark_session, client, processor_from_time, to_time, overwrite)
                df_name, df_process_duration, df_total_records, conversion_progress_success, process_time = etl.process_data()
                metric_utils = MetricUtils()
                metric_utils.create_gauge_metrics(df_name, df_process_duration, df_total_records, process_time)

                if conversion_progress_success == False:                    
                    last_fail_dict[processor.__name__] = str_from_datetime(processor_from_time)
                else:
                    last_fail_dict.pop(processor.__name__, None)

            et = datetime.now(tz=settings.timezone)
            print(f"End Time: {et}")
            print(f"Total Duration: {(et - st).total_seconds()}")
            print(f"================END: OGG_{self.app_name}_ETL================\n")

        except Exception as ETL_PROCESS_EXCEPTION:
            print("ETL PROCESSES RESULT IN THE FOLLOWING ERROR:", ETL_PROCESS_EXCEPTION)

        return last_fail_dict
