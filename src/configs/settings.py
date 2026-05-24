import os
import pytz
from dotenv import load_dotenv

class Settings():
    load_dotenv()
    
    # Defaults
    pushgateway: str = os.getenv("PUSHGATEWAY_URL")
    env:str = os.getenv("ENV")
    tz:str = os.getenv("TZ", default="Asia/Ho_Chi_Minh")
    timezone = pytz.timezone(tz)
    
    # Secret manager
    sm_url:str = os.getenv("SECRET_MANAGER_URL")
    sm_app_id:str = os.getenv("SM_DL_S3_APP_ID")
    sm_query:str = os.getenv("SM_DL_S3_SAFE")
    sm_object_name:str = os.getenv("SM_DL_S3_OBJECT")
    
    # S3
    s3_url:str = os.getenv("S3_URL")
    s3_port:str = os.getenv("S3_PORT")
    s3_access_key:str = os.getenv("S3_ACCESS_KEY")
    s3_secret_key:str = os.getenv("S3_SECRET_KEY")
    s3_batch_size:int = int(os.getenv("S3_BATCH_SIZE"))
    
    s3_bucket_name:str = os.getenv("S3_BUCKET_NAME")
    s3_bo_source_path:str = os.getenv("S3_BO_SOURCE_PATH")
    s3_bo_full_destination_path:str = os.getenv("S3_BO_FULL_DESTINATION_PATH")

    s3_source_account_path:str = os.getenv("S3_FULL_SOURCE_ACCOUNT_PATH")
    s3_source_customer_path:str = os.getenv("S3_FULL_SOURCE_CUSTOMER_PATH")
    s3_source_mkt_path:str = os.getenv("S3_FULL_SOURCE_MKT_PATH")

    s3_dest_account_hash_path:str = os.getenv("S3_DEST_ACCOUNT_HASH_PATH")
    s3_dest_customer_hash_path:str = os.getenv("S3_DEST_CUSTOMER_HASH_PATH")
    s3_dest_mkt_hash_path:str = os.getenv("S3_DEST_MKT_HASH_PATH")


    
    # Spark
    spark_url:str = os.getenv("SPARK_URL")
    spark_driver_host:str = os.getenv("SPARK_SUBMIT_PUBLIC_IP")
    spark_driver_bind_address:str = os.getenv("SPARK_SUBMIT_LOCAL_IP")
    spark_driver_port:str = os.getenv("SPARK_DRIVER_PORT")
    spark_block_manager_port:str = os.getenv("SPARK_BLOCK_MANAGER_PORT")
    spark_driver_block_manager_port:str = os.getenv("SPARK_DRIVER_BLOCK_MANAGER_PORT")
    spark_ui_port:str = os.getenv("SPARK_DRIVER_UI_PORT")
    spark_driver_memory:str = os.getenv("SPARK_DRIVER_MEMORY")
    spark_executor_memory:str = os.getenv("SPARK_EXECUTOR_MEMORY")
    
    # Date string format
    date_format:str = "%Y-%m-%d"
    datetime_format:str = "%Y-%m-%d %H:%M:%S"
    datetime_full_format:str = "%Y-%m-%d %H:%M:%S.%f"
    
    # Hudi
    hudi_table_type = "COPY_ON_WRITE"
    hudi_parallelism = 8
    hudi_small_file_limit = 50 * 1024 * 1024
    hudi_max_file_size = 60 * 1024 * 1024
    hudi_precombine_field = "current_ts"
    
    # Hive
    hive_metastore_url:str = os.getenv("HIVE_METASTORE_URL")
    hive_sync_database:str = os.getenv("HIVE_SYNC_DATABASE")
    hive_sync_enable = "true"
    hive_sync_mode = "hms"

    # Files Prefix according to naming convention
    snapshot_prefix = 'INIT.'
    cdc_prefix = 'BACK.'

    # Mapping settings
    jdbc_url = os.getenv("JDBC_URL")
    target_table = os.getenv("TARGET_TABLE")
    map_pg_usr = os.getenv("MAP_PG_USR")
    map_pg_secret = os.getenv("MAP_PG_SECRET")
    map_pg_host = os.getenv("MAPPING_PG_HOST")
    map_pg_port = os.getenv("MAPPING_PG_PORT")
    map_pg_database = os.getenv("MAPPING_PG_DATABASE")
    driver = "org.postgresql.Driver"
    
settings = Settings()