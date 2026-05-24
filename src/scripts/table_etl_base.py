from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from minio import Minio
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import lit, when, col, to_timestamp, unix_micros
from pyspark.sql.types import DateType, TimestampNTZType, TimestampType
from functools import reduce


from src.helpers.ogg_utils import OGGCDCProcessor, OGGOperationType
from src.helpers.s3_utils import S3Utils
from src.configs.settings import settings
from src.helpers.dag_utils import file_matching
from src.helpers.hash_hudi_utils import gen_uuid, call_pg_function_partition

TIMEZONE_OFFSET_HOURS = 7
time_types = (DateType, TimestampNTZType, TimestampType)
offset = timedelta(hours=TIMEZONE_OFFSET_HOURS)

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
                df.sort(col("current_ts"))
                return df

        return None

    def load_data(self, upsert_data:DataFrame, delete_data:DataFrame, spark_write_mode):
        if upsert_data is not None:
            print("Load dataframe...")
            print("|______  Handling upsert op...")
            print("|____________  Upsert df")
            print("|_______________  Count:", upsert_data.count())

            if not upsert_data.isEmpty():
                hudi_options = self.create_hudi_options("upsert")
                upsert_data.write.format("hudi").options(**hudi_options).mode(spark_write_mode).save(self.link_destination_path)

            print("|______  Handling delete op...")
            print("|____________  Delete df")
            print("|_______________  Count:", delete_data.count())

            if not delete_data.isEmpty():
                hudi_options = self.create_hudi_options("delete")

                # Force set write mode append
                spark_write_mode = "append"
                delete_data.write.format("hudi").options(**hudi_options).mode(spark_write_mode).save(self.link_destination_path)

        del upsert_data
        del delete_data

    def transform_data(self, raw_data:DataFrame) -> DataFrame:
        print("Begin Transform...")
        ogg_processor = OGGCDCProcessor()

        insert_op_type = OGGOperationType.INSERT.value
        update_op_type = OGGOperationType.UPDATE.value
        delete_op_type = OGGOperationType.DELETE.value
        snapshot_op_type = OGGOperationType.INITIAL.value
        insert_df = raw_data.filter(col("op_type") == insert_op_type)
        update_df = raw_data.filter(col("op_type") == update_op_type)
        delete_df = raw_data.filter(col("op_type") == delete_op_type)
        init_df = raw_data.filter(col("op_type") == snapshot_op_type)

        print("'--- Begin to flatten insert df...")
        if not insert_df.isEmpty():
            insert_df = ogg_processor.flat_dataframe(insert_df, insert_op_type)
        print("'--- Finish to flatten insert df...")

        print("'--- Begin to flatten update df...")
        if not update_df.isEmpty():
            update_df = ogg_processor.flat_dataframe(update_df, update_op_type)
        print("'--- Finish to flatten update df...")

        print("'--- Begin to flatten delete df...")
        if not delete_df.isEmpty():
            delete_df = ogg_processor.flat_dataframe(delete_df, delete_op_type)
        print("'--- Finish to flatten delete df...")

        print("'--- Begin to flatten init df...")
        if not init_df.isEmpty():
            init_df = ogg_processor.flat_dataframe(init_df, snapshot_op_type)
            print("'------ Setting current_ts to minimal time...")
            init_df = init_df.withColumn("current_ts", unix_micros(to_timestamp(lit("2026-01-01 07:00:00"))))

            print("'------ Finish to set minial current_ts value for snapshot data...")
        print("'--- Finish to flatten init df...")


        print("'--- Joining Upserts...")
        upsert_dfs = [insert_df, update_df, init_df]
        upsert_dfs = [df for df in upsert_dfs if not df.isEmpty()]
        final_upsert_df = reduce(DataFrame.unionAll, upsert_dfs)
        final_upsert_df.sort(col("current_ts"))

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
                final_upsert_df = final_upsert_df.withColumn(field.name, when(col(field.name) < marked_time, marked_time).otherwise(final_upsert_df[field.name]))
        print("Finish filter timestamp cols...")

        #TODO: Logic check if c_customer_code and c_account_code in upcoming upsert batch exists in map table or not
        
        jdbc_props = {
            "user": settings.map_pg_usr,
            "password": settings.map_pg_secret,
            "driver": "org.postgresql.Driver"
        }

        postgres_df = (
            self.spark_session.read
                .jdbc(
                    url=settings.jdbc_url,
                    table='''
                    (
                        SELECT "C_CUSTOMER_CODE"
                        FROM public.t_cust_customer_map
                    ) tmp
                    ''',
                    properties=jdbc_props
                )
        )

        existing_df = df.join(postgres_df, df.C_CUSTOMER_CODE==postgres_df.C_CUSTOMER_CODE, "inner").drop(postgres_df["C_CUSTOMER_CODE"])
        not_existing_cust_df = final_upsert_df.join(postgres_df,final_upsert_df.C_CUSTOMER_CODE==postgres_df.C_CUSTOMER_CODE,how="left_anti")
        
        # Handle not existing_df
        if not not_existing_cust_df.isEmpty():
            df_new_records_with_uuid = not_existing_cust_df.withColumn("C_CUSTOMER_CODE_MAP", gen_uuid())
            df_new_records_with_uuid.foreachPartition(call_pg_function_partition)

        return final_upsert_df, delete_df

    @abstractmethod
    def create_hudi_options(self, write_operation) -> dict:
        pass