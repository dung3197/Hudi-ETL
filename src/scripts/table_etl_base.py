from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from minio import Minio
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, to_timestamp, unix_micros
from pyspark.sql.types import DateType, TimestampNTZType, TimestampType
from functools import reduce
from typing import Optional, Tuple


from src.helpers.ogg_utils import OGGCDCProcessor, OGGOperationType
from src.helpers.s3_utils import S3Utils
from src.configs.settings import settings
from src.helpers.dag_utils import file_matching
from src.helpers.hash_hudi_utils import gen_uuid

TIMEZONE_OFFSET_HOURS = 7
time_types = (DateType, TimestampNTZType, TimestampType)
offset = timedelta(hours=TIMEZONE_OFFSET_HOURS)
CUSTOMER_CODE_COLUMN = "C_CUSTOMER_CODE"
CUSTOMER_CODE_MAP_COLUMN = "C_CUSTOMER_CODE_MAP"
CUSTOMER_MAP_TABLE = "public.t_cust_customer_map"

class TableETLBase(ABC):

    def __init__(self, spark_session: SparkSession, s3_client:Minio, from_time:datetime, to_time:datetime, overwrite:bool, source_table:str, source_path:str, link_destination_path:str):

        self.spark_session = spark_session
        self.from_time = from_time
        self.to_time = to_time
        self.overwrite = overwrite
        self.source_table = source_table

        self.source_path = source_path
        self.link_destination_path = link_destination_path

        self.s3_utils = S3Utils(s3_client)


    def process_data(self):
        print(f"----------------{self.source_table}----------------")
        print(f"start etl {self.source_table}")
        process_data_success = True
        total_duration = 0
        total_records = 0
        st = datetime.now(tz=settings.timezone)
        try:
            print(f"Start Time: {st}")
            print("Finding new files between:", self.from_time, "and:", self.to_time)
            all_files = self.s3_utils.list_objects_by_range(settings.s3_bucket_name, self.source_path, self.from_time, self.to_time)
            print('All_files:', all_files)

            if len(all_files) > 0:
                file_batches = [all_files[i:i + settings.s3_batch_size] for i in range(0, len(all_files), settings.s3_batch_size)]
                spark_write_mode = "overwrite" if self.overwrite == True else "append"
                batch_idx = 0

                if len(file_batches) > 1:
                    print(f"Total batches: {len(file_batches)}")

                for objs in file_batches:
                    if len(file_batches) > 1:
                        print(f"Start batch: {batch_idx}")

                    cdc_data = self.extract_data(objs)
                    print("Finished Extract")
                    batch_count = 0

                    if cdc_data is not None:
                        upsert_data, delete_data = self.transform_data(cdc_data)
                        print("Finished Transform")

                        self.load_data(upsert_data, delete_data, spark_write_mode)
                        print("Finished Load")

                        batch_count = cdc_data.count()
                        total_records += batch_count

                        #reset write mode
                        spark_write_mode = "append"
                        del upsert_data, delete_data

                    if len(file_batches) > 1:
                        print(f"End batch: {batch_idx} - Total batch records: {batch_count:,}")
                    batch_idx += 1

                    del cdc_data

            if total_records > 0:
                print(f"Total records: {total_records:,}")
            else:
                print("No data change")
        except Exception as e:
            print(f"Error while ETL data {self.source_table}. Details: {e}")
            process_data_success = False

        et = datetime.now(tz=settings.timezone)
        total_duration = (et - st).total_seconds()
        print(f"End Time: {et}")
        print(f"Total Duration: {total_duration}")
        print("------------------------------------------------\n")

        return self.source_table, total_duration, total_records, process_data_success, st

    def extract_data(self, objs)-> DataFrame:
        if len(objs) > 0:

            print("Begin Extract...")

            # Filter right away which files are snapshot files and those are not.
            snap_paths, cdc_paths = file_matching(objs)

            df = df_snap = df_cdc = None
            
            if cdc_paths != []:
                df_cdc:DataFrame = self.spark_session.read.format("avro").load(cdc_paths)

            if snap_paths != []:

                # Pre-process current_ts and op_ts to standardize schema structure along with CDC's
                df_snap:DataFrame = self.spark_session.read.format("avro").load(snap_paths).withColumn("current_ts", unix_micros("current_ts")).withColumn("op_ts", unix_micros("op_ts"))

            if cdc_paths != [] and snap_paths != []:
                df = df_snap.unionByName(df_cdc)
            else:
                df = df_snap or df_cdc

            if df != None:
                return df.sort(col("current_ts"))

        return None

    def load_data(self, upsert_data:DataFrame, delete_data:DataFrame, spark_write_mode):
        if upsert_data is not None or delete_data is not None:
            print("Load dataframe...")
            if upsert_data is not None:
                print("|______  Handling upsert op...")
                print("|____________  Upsert df")
                print("|_______________  Count:", upsert_data.count())

                if not upsert_data.isEmpty():
                    hudi_options = self.create_hudi_options("upsert")
                    upsert_data.write.format("hudi").options(**hudi_options).mode(spark_write_mode).save(self.link_destination_path)

            if delete_data is not None:
                print("|______  Handling delete op...")
                print("|____________  Delete df")
                print("|_______________  Count:", delete_data.count())

                if not delete_data.isEmpty():
                    hudi_options = self.create_hudi_options("delete")

                    # Force delete operations to append so an upsert overwrite batch cannot erase existing Hudi data.
                    spark_write_mode = "append"
                    delete_data.write.format("hudi").options(**hudi_options).mode(spark_write_mode).save(self.link_destination_path)

        del upsert_data
        del delete_data

    def transform_data(self, raw_data:DataFrame) -> Tuple[DataFrame, DataFrame]:
        print("Begin Transform...")
        ogg_processor = OGGCDCProcessor()

        insert_op_type = OGGOperationType.INSERT.value
        update_op_type = OGGOperationType.UPDATE.value
        delete_op_type = OGGOperationType.DELETE.value
        snapshot_op_type = OGGOperationType.INITIAL.value

        # Split the raw OGG dataframe by operation type so each branch can be flattened with the right before/after payload.
        raw_insert_df = raw_data.filter(col("op_type") == insert_op_type)
        raw_update_df = raw_data.filter(col("op_type") == update_op_type)
        raw_delete_df = raw_data.filter(col("op_type") == delete_op_type)
        raw_init_df = raw_data.filter(col("op_type") == snapshot_op_type)

        insert_df = self._flatten_ogg_dataframe_if_not_empty(ogg_processor, raw_insert_df, insert_op_type, "insert")
        update_df = self._flatten_ogg_dataframe_if_not_empty(ogg_processor, raw_update_df, update_op_type, "update")
        delete_df = self._flatten_ogg_dataframe_if_not_empty(ogg_processor, raw_delete_df, delete_op_type, "delete")
        if delete_df is None:
            # Keep an empty delete dataframe available for load_data count/isEmpty checks without mapping customer codes.
            delete_df = raw_delete_df.limit(0)

        init_df = self._flatten_ogg_dataframe_if_not_empty(ogg_processor, raw_init_df, snapshot_op_type, "init")
        if init_df is not None:
            print("'------ Setting current_ts to minimal time...")
            init_df = init_df.withColumn("current_ts", unix_micros(to_timestamp(lit("2026-01-01 07:00:00"))))
            print("'------ Finish to set minimal current_ts value for snapshot data...")


        print("'--- Joining Upserts...")
        upsert_dfs = [df for df in [insert_df, update_df, init_df] if df is not None]
        if upsert_dfs:
            # Union by column name so insert/update/init branches stay aligned even if Spark returns fields in a different order.
            final_upsert_df = reduce(lambda left_df, right_df: left_df.unionByName(right_df), upsert_dfs)
            final_upsert_df = final_upsert_df.sort(col("current_ts"))
            final_upsert_df = self._normalize_time_columns(final_upsert_df)
            final_upsert_df = self._apply_customer_code_mapping_if_needed(final_upsert_df)
        else:
            # Return an empty flat dataframe so downstream table-specific transforms can still see the source schema.
            print("'--- No upsert rows found; skipping upsert timestamp and customer-code transforms...")
            final_upsert_df = self._empty_flat_dataframe(raw_data)

        return final_upsert_df, delete_df

    def _flatten_ogg_dataframe_if_not_empty(
        self,
        ogg_processor: OGGCDCProcessor,
        ogg_df: DataFrame,
        op_type: str,
        op_name: str
    ) -> Optional[DataFrame]:
        print(f"'--- Begin to flatten {op_name} df...")
        if ogg_df.isEmpty():
            print(f"'--- No {op_name} rows found; skip flattening...")
            print(f"'--- Finish to flatten {op_name} df...")
            return None

        # OGG insert/update/init rows use the after payload; delete rows use the before payload.
        flattened_df = ogg_processor.flat_dataframe(ogg_df, op_type)
        print(f"'--- Finish to flatten {op_name} df...")
        return flattened_df

    def _empty_flat_dataframe(self, raw_data: DataFrame) -> DataFrame:
        # Build a zero-row dataframe with flattened business columns for delete-only batches.
        after_fields = raw_data.schema["after"].dataType.fieldNames() if "after" in raw_data.columns else []
        before_fields = raw_data.schema["before"].dataType.fieldNames() if "before" in raw_data.columns else []
        business_struct = "after" if len(after_fields) > 0 else "before"
        business_fields = after_fields if len(after_fields) > 0 else before_fields

        # Preserve the metadata columns such as op_type/current_ts while removing nested before/after structs.
        business_exprs = [col(f"{business_struct}.{field}").alias(field) for field in sorted(business_fields)]
        metadata_exprs = [col(name) for name in raw_data.columns if name not in ["before", "after"]]
        return raw_data.limit(0).select(*(business_exprs + metadata_exprs))

    def _normalize_time_columns(self, final_upsert_df: DataFrame) -> DataFrame:
        # Shift source date/timestamp values to the project timezone offset before Hudi writes.
        for field in final_upsert_df.schema.fields:
            if isinstance(field.dataType, time_types):
                col_name = field.name
                if isinstance(field.dataType, DateType):
                    final_upsert_df = final_upsert_df.withColumn(col_name, to_timestamp(col(col_name)) + lit(offset))
                else:
                    final_upsert_df = final_upsert_df.withColumn(col_name, col(col_name) + lit(offset))

        fields_list = final_upsert_df.schema.fields
        marked_time = lit('0001-01-01 00:00:00').cast(TimestampType())

        print("Begin to filter timestamp column")
        for field in fields_list:
            if isinstance(field.dataType, TimestampType):
                # Keep out-of-range timestamp values from breaking downstream storage/readers.
                final_upsert_df = final_upsert_df.withColumn(field.name, when(col(field.name) < marked_time, marked_time).otherwise(final_upsert_df[field.name]))
        print("Finish filter timestamp cols...")
        return final_upsert_df

    def _apply_customer_code_mapping_if_needed(self, final_upsert_df: DataFrame) -> DataFrame:
        # Only upsert dataframes containing the sensitive customer code need PostgreSQL mapping.
        if CUSTOMER_CODE_COLUMN in final_upsert_df.columns:
            print(f"'--- Begin replacing {CUSTOMER_CODE_COLUMN} with {CUSTOMER_CODE_MAP_COLUMN}...")
            final_upsert_df = self._replace_customer_code_with_map(final_upsert_df)
            print(f"'--- Finish replacing {CUSTOMER_CODE_COLUMN} with {CUSTOMER_CODE_MAP_COLUMN}...")
        else:
            print(f"'--- {CUSTOMER_CODE_COLUMN} column not found; skipping customer-code mapping...")
        return final_upsert_df

    def _read_customer_map(self) -> DataFrame:
        # Read only the two required mapping columns from PostgreSQL to avoid loading unnecessary table data.
        jdbc_props = {
            "user": settings.map_pg_usr,
            "password": settings.map_pg_secret,
            "driver": settings.driver
        }

        customer_map_query = f'''
                    (
                        SELECT
                            "{CUSTOMER_CODE_COLUMN}",
                            "{CUSTOMER_CODE_MAP_COLUMN}"
                        FROM {CUSTOMER_MAP_TABLE}
                    ) customer_map
                    '''

        return (
            self.spark_session.read
                .jdbc(
                    url=settings.jdbc_url,
                    table=customer_map_query,
                    properties=jdbc_props
                )
                .where(col(CUSTOMER_CODE_COLUMN).isNotNull())
                .dropDuplicates([CUSTOMER_CODE_COLUMN])
        )

    def _replace_customer_code_with_map(self, final_upsert_df: DataFrame) -> DataFrame:
        # Load the latest PostgreSQL customer map every time transform_data handles a C_CUSTOMER_CODE upsert dataframe.
        customer_map_df = self._read_customer_map()

        # Keep only unique, non-null customer codes from the incoming upsert batch before checking PostgreSQL.
        incoming_customer_codes_df = (
            final_upsert_df
                .select(CUSTOMER_CODE_COLUMN)
                .where(col(CUSTOMER_CODE_COLUMN).isNotNull())
                .distinct()
        )

        # Find only incoming customer codes that are not already present in the PostgreSQL map table.
        missing_customer_codes_df = incoming_customer_codes_df.join(
            customer_map_df.select(CUSTOMER_CODE_COLUMN),
            on=CUSTOMER_CODE_COLUMN,
            how="left_anti"
        )

        if not missing_customer_codes_df.isEmpty():
            # Generate mapped values only for missing codes and let PostgreSQL ignore duplicate insert conflicts safely.
            new_customer_map_df = missing_customer_codes_df.withColumn(CUSTOMER_CODE_MAP_COLUMN, gen_uuid())
            self._insert_customer_map_on_driver(new_customer_map_df)

            # Re-read PostgreSQL after inserts so concurrent conflict winners provide the authoritative map value.
            customer_map_df = self._read_customer_map()

        # Join the authoritative mapped value into the upsert dataframe and remove any stale map column from the source.
        mapped_upsert_df = (
            final_upsert_df
                .drop(CUSTOMER_CODE_MAP_COLUMN)
                .join(customer_map_df, on=CUSTOMER_CODE_COLUMN, how="left")
        )

        unmapped_customer_df = (
            mapped_upsert_df
                .where(col(CUSTOMER_CODE_COLUMN).isNotNull() & col(CUSTOMER_CODE_MAP_COLUMN).isNull())
                .select(CUSTOMER_CODE_COLUMN)
                .limit(1)
        )
        if not unmapped_customer_df.isEmpty():
            raise ValueError(f"{CUSTOMER_MAP_TABLE} did not return a mapped value for at least one incoming {CUSTOMER_CODE_COLUMN}")

        # Drop the sensitive source value after every non-null customer code has a map value.
        return mapped_upsert_df.drop(CUSTOMER_CODE_COLUMN)

    def _insert_customer_map_on_driver(self, new_customer_map_df: DataFrame):
        # Keep PostgreSQL writes on the driver so Spark executors do not need psycopg2 installed.
        import psycopg2
        from psycopg2.extras import execute_batch

        sql = """
            SELECT public.safe_insert_t_cust_customer_map(
                %s, %s
            )
        """
        batch = []
        conn = None
        cur = None

        try:
            conn = psycopg2.connect(
                host=settings.map_pg_host,
                port=settings.map_pg_port,
                database=settings.map_pg_database,
                user=settings.map_pg_usr,
                password=settings.map_pg_secret
            )
            conn.autocommit = False
            cur = conn.cursor()

            # Stream Spark rows to the driver instead of collecting everything into memory at once.
            for row in new_customer_map_df.select(CUSTOMER_CODE_COLUMN, CUSTOMER_CODE_MAP_COLUMN).toLocalIterator():
                batch.append((
                    row[CUSTOMER_CODE_COLUMN],
                    row[CUSTOMER_CODE_MAP_COLUMN],
                ))

                if len(batch) >= 5000:
                    execute_batch(cur, sql, batch, page_size=5000)
                    conn.commit()
                    batch.clear()

            if batch:
                execute_batch(cur, sql, batch, page_size=5000)
                conn.commit()
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    @abstractmethod
    def create_hudi_options(self, write_operation) -> dict:
        pass
