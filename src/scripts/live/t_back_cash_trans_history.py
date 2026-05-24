from minio import Minio
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import year, to_timestamp, col, to_date

from src.scripts.live.bo_table_etl_base import BOETLBase
from src.helpers.hudi_utils import create_hudi_options

class T_BACK_CASH_TRANS_HISTORY(BOETLBase):

    def __init__(self, spark_session: SparkSession, s3_client:Minio, from_time:datetime, to_time:datetime, overwrite:bool):
        super().__init__(spark_session, s3_client, from_time, to_time, overwrite, "T_BACK_CASH_TRANS_HISTORY")

    def transform_data(self, raw_data:DataFrame) -> DataFrame:
        upsert_data, delete_data = super().transform_data(raw_data)
        upsert_data = upsert_data.na.drop(subset=["C_TRANSACTION_DATE"])
        upsert_data = upsert_data.withColumn("C_TRANSACTION_DATE_PARTITION", to_date(col("C_TRANSACTION_DATE")))
        upsert_data = upsert_data.na.drop(subset=["PK_CASH_TRANSACTION"])
        return upsert_data, delete_data
    
    def create_hudi_options(self, write_operation) -> dict:
        """Partion field: 
           Sort field: C_TRANSACTION_DATE ( Ngày bút toán )
           Clustering field: 
        """
        return create_hudi_options("COPY_ON_WRITE", self.source_table, write_operation, "PK_CASH_TRANSACTION", "C_TRANSACTION_DATE_PARTITION", False, "C_ACCOUNT_CODE", "C_ACCOUNT_CODE")

