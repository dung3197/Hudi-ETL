from pyspark.sql import SparkSession
from src.configs.settings import settings
import uuid
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import psycopg2
from psycopg2.extras import execute_batch


# Keep the original uuid.uuid4().hex[:20] format, but create the Spark UDF lazily.
# Airflow can import this module before SparkSession exists; gen_uuid() is called later during dataframe transforms.
def gen_uuid():
    def _gen_uuid():
        return str(uuid.uuid4().hex[:20])

    # Use the keyword returnType form because some PySpark versions parse positional
    # StringType() incorrectly as SQL text, which can raise PARSE_SYNTAX_ERROR.
    return udf(_gen_uuid, returnType=StringType())()


def call_pg_function_partition(rows):
    conn = psycopg2.connect(
        host=settings.map_pg_host,
        port=settings.map_pg_port,
        database=settings.map_pg_database,
        user=settings.map_pg_usr,
        password=settings.map_pg_secret
    )
    
    conn.autocommit = False
    cur = conn.cursor()

    sql = """
        SELECT public.safe_insert_t_cust_customer_map(
            %s, %s
        )
    """

    batch = []

    for r in rows:
        batch.append((
            r["C_CUSTOMER_CODE"],
            r["C_CUSTOMER_CODE_MAP"],
        ))

        if len(batch) >= 5000:
            execute_batch(cur, sql, batch, page_size=5000)
            conn.commit()
            batch.clear()

    if batch:
        execute_batch(cur, sql, batch, page_size=5000)
        conn.commit()

    cur.close()
    conn.close()


def create_session(app_name, s3_conn:dict)-> SparkSession:
    session_builder = SparkSession.builder \
                            .master(settings.spark_url) \
                            .appName(app_name) \
                            .config("spark.sql.session.timeZone", "UTC") \
                            .config("spark.log.level", "ERROR") \
                            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                            .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
                            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
                            .config("spark.hadoop.fs.s3a.endpoint", s3_conn['s3_url']) \
                            .config("spark.hadoop.fs.s3a.access.key", s3_conn['s3_access_key']) \
                            .config("spark.hadoop.fs.s3a.secret.key", s3_conn['s3_secret_key']) \
                            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                            .config("spark.sql.avro.datetimeRebaseModeInRead", "CORRECTED") \
                            .config("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED") \
                            .config("spark.sql.avro.int96RebaseModeInRead", "CORRECTED") \
                            .config("spark.sql.avro.int96RebaseModeInWrite", "CORRECTED") \
                            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
                            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
                            .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
                            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
                            .config("spark.executor.cores", 2) \
                            .config("spark.executor.memory", "2048m") \
                            .config("spark.executor.instances", 3) \
                            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                            .config("spark.yarn.queue", "datalake")  \
                            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")


# .config("spark.yarn.queue", "datalake")  \
    if settings.env == "local":
        spark_session:SparkSession = session_builder.getOrCreate()
    else:
        spark_session:SparkSession = session_builder\
                        .config("spark.driver.host", settings.spark_driver_host) \
                        .config("spark.driver.bindAddress", settings.spark_driver_bind_address) \
                        .config("spark.driver.port", settings.spark_driver_port) \
                        .config("spark.blockManager.port", settings.spark_block_manager_port) \
                        .config("spark.driver.blockManager.port", settings.spark_driver_block_manager_port) \
                        .config("spark.ui.port", settings.spark_ui_port) \
                        .getOrCreate()

    return spark_session

def create_hudi_options(table_type, table_name, write_operation, recordkey, partition, drop_partition_column:bool, sort_columns=None, clustering_columns=None) -> dict:
    hudi_options = {
        # Hudi table properties
        "className": "org.apache.hudi",
        # "hoodie.embed.timeline.server.port": self.hudi_timeline_server_port,
        "hoodie.table.type": table_type,
        "hoodie.table.name": table_name, #+"_new",
        "hoodie.datasource.write.table.name": table_name, #+"_new",
        "hoodie.datasource.write.operation": write_operation,
        # "hoodie.datasource.write.partitionpath.field": "data_date", # Partition
        # "hoodie.datasource.write.recordkey.field": "Id", # Record key

        "hoodie.datasource.write.schema.evolution.enable": "true",
        "hoodie.datasource.write.precombine.field": settings.hudi_precombine_field,
        "hoodie.table.precombine.field": settings.hudi_precombine_field,

        "hoodie.upsert.shuffle.parallelism": settings.hudi_parallelism,
        "hoodie.insert.shuffle.parallelism": settings.hudi_parallelism,

        # Hudi file sizing
        "hoodie.parquet.small.file.limit": settings.hudi_small_file_limit,
        "hoodie.parquet.max.file.size": settings.hudi_max_file_size,

        # # excluded cdc columns
        # "hoodie.datasource.write.excluded.columns": "__deleted, _source_ts_ms, _event_lsn, _table, _op",

        # Hudi Cleaning
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "false",
        "hoodie.cleaner.policy": "KEEP_LATEST_FILE_VERSIONS",
        "hoodie.cleaner.fileversions.retained": "1",
        "hoodie.cleaner.parallelism": "200",

        # Hudi reconcile schema
        "hoodie.datasource.write.reconcile.schema": "true",

        # Test 
        # "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
        "hoodie.metadata.index.column.stats.enable": "true",
        "hoodie.upsert.shuffle.parallelism": 5,
        # "hoodie.table.cdc.enabled": "true",
        # "hoodie.table.cdc.supplemental.logging.mode": "data_before_after",
    }

    if partition is not None:
        hudi_options["hoodie.datasource.write.partitionpath.field"] = partition
        if drop_partition_column:
            hudi_options["hoodie.datasource.write.drop.partition.columns"] = "true"
    if recordkey is not None:
        hudi_options["hoodie.datasource.write.recordkey.field"] = recordkey

    if sort_columns is not None:
        hudi_options["hoodie.clustering.plan.strategy.sort.columns"] = sort_columns
    # if excluded_columns is not None:
    #     hudi_options["hoodie.datasource.write.excluded.columns"] += f",{excluded_columns}"

    if clustering_columns is not None:
        hudi_options.update({
            # Hudi Clustering
            "hoodie.clustering.inline": "true",
            "hoodie.clustering.inline.max.commits": "1",
            "hoodie.clustering.plan.strategy.target.file.max.bytes": "125829120", # 1073741824
            "hoodie.clustering.plan.strategy.small.file.limit": "104857600", # 629145600 125829120
            "hoodie.clustering.execution.strategy.class": "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
            "hoodie.clustering.plan.strategy.sort.columns": clustering_columns,
            "hoodie.layout.optimize.enable": "true"
        })



    # if settings.env != "local":
    hudi_options.update({
        # Hive Sync
        "hoodie.datasource.hive_sync.metastore.uris": settings.hive_metastore_url,
        "hoodie.datasource.hive_sync.mode": settings.hive_sync_mode,
        "hoodie.datasource.hive_sync.enable": settings.hive_sync_enable,
        "hoodie.datasource.hive_sync.database": settings.hive_sync_database,
        "hoodie.datasource.hive_sync.table": table_name, #+"_new",
        "hoodie.datasource.hive_sync.support_timestamp": "true",
        "hoodie.metadata.enable": "true",
        "hoodie.index.type": "RECORD_INDEX",
        "hoodie.schema.on.read.enable": "true"
    })

    return hudi_options
