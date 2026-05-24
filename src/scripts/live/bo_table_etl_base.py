from datetime import datetime
from minio import Minio
from pyspark.sql import SparkSession

from src.configs.settings import settings
from src.scripts.table_etl_base import TableETLBase


class BOETLBase(TableETLBase):
    def __init__(self, spark_session: SparkSession, s3_client:Minio, from_time:datetime, to_time:datetime, overwrite:bool, source_table:str):
        source_path = f"{settings.s3_bo_source_path}/BACK.{source_table}"
        link_destination_path = f"s3a://{settings.s3_bo_full_destination_path}/{source_table}"
        
        super().__init__(spark_session, s3_client, from_time, to_time, overwrite, source_table, source_path, link_destination_path)