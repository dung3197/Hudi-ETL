from pyspark.sql.functions import current_timestamp, from_utc_timestamp, col, concat, udf
import uuid
from src.configs.secret_manager import get_s3_conn
from src.helpers.hash_hudi_utils import create_hudi_options, create_session
from pyspark.sql.functions import col, substring
from src.configs.settings import settings

app_name = "ETL BO RAW - HASH ACCOUNT"

def account_infor_warehouse_hash():
    s3_conn = get_s3_conn()
    spark = create_session(app_name, s3_conn)

    def result_hudi_options(source_table, write_operation) -> dict:
        return create_hudi_options("COPY_ON_WRITE", source_table, write_operation, "C_ACCOUNT_CODE", None, False, "C_ACCOUNT_CODE,C_CUSTOMER_CODE", "C_ACCOUNT_CODE")

    # ===================== #
    # 1. Đọc bảng gốc
    # ===================== #
    source_path = f"s3a://{settings.s3_source_account_path}"
    target_path = f"s3a://{settings.s3_dest_account_hash_path}"

    cust_map_path = f"s3a://{settings.s3_dest_customer_hash_path}"

    account_df_source = spark.read.format("hudi").load(source_path)
    cust_map_df = spark.read.format("hudi").load(cust_map_path)


    print("|__________ Dropping NULL C_ACCOUNT_CODE...")
    df_source_drop_na = account_df_source.na.drop(subset=["C_ACCOUNT_CODE"])
    df_source_new = df_source_drop_na.select("C_CUSTOMER_CODE", "C_ACCOUNT_CODE", "op_type","current_ts")


    # ================================================= #
    # 2. Join bảng
    # ================================================= #
    join_df = df_source_new.join(cust_map_df.select("C_CUSTOMER_CODE", "C_CUSTOMER_CODE_MAP"), on="C_CUSTOMER_CODE", how="left")


    # ================================================= #
    # 3. Kiểm tra bảng đích đã tồn tại chưa
    # ================================================= #
    try:
        df_target = spark.read.format("hudi").load(target_path)
        if df_target.count() > 0:
            table_exists = True
        else: 
            table_exists = False
    except Exception:
      table_exists = False


    # ===================== #
    # 4. UDF sinh UUID
    # ===================== #
    @udf("string")
    def gen_uuid():
        return str(uuid.uuid4().hex[:20])


    # ================================================= #
    # 5. Ghi bảng map account mới
    # ================================================= #
    if not table_exists == True:
        print("===> Initializing new map table for t_back_account with full clone...")
        
        init_update_time = from_utc_timestamp(current_timestamp(), "Asia/Ho_Chi_Minh")
        df_map = join_df.withColumn("ACCOUNT_SUFFIX", substring(col("C_ACCOUNT_CODE"), -1, 1)) \
        .withColumn("C_ACCOUNT_CODE_MAP", concat(col("C_CUSTOMER_CODE_MAP"), col("ACCOUNT_SUFFIX"))) \
        .withColumn("WH_UPDATE_TIME", init_update_time)

        df_map_init = df_map.drop("ACCOUNT_SUFFIX")

        hudi_options = result_hudi_options("T_BACK_ACCOUNT_MAP", "upsert")
        
        df_map_init.write \
            .format("hudi") \
            .options(**hudi_options) \
            .mode("overwrite") \
            .save(target_path)


    # ============================ #
    # 6. Incremental Update
    # ============================ #
    else:
        print("===> Incremental sync mode...")

        # Lấy C_ACCOUNT_CODE từ bảng đích
        print("|__________ Getting dest account code list...")
        df_existing_ids = df_target.select("C_ACCOUNT_CODE").distinct()

        print("|__________ Getting newly added account code list...")
        # Lọc record mới xuất hiện trong bảng gốc
        df_new_records = join_df.join(df_existing_ids, "C_ACCOUNT_CODE", "leftanti")
        print("|__________ Finished filtering...")

        if df_new_records.count() > 0:
            print("|_________________ Begin to generate UUID and update time...")
            incremental_update_time = from_utc_timestamp(current_timestamp(), "Asia/Ho_Chi_Minh")
            df_new_records_with_uuid = df_new_records.withColumn("C_ACCOUNT_CODE_MAP", gen_uuid()).withColumn("WH_UPDATE_TIME", incremental_update_time)
            df_new_records_with_uuid.select("C_ACCOUNT_CODE", "C_ACCOUNT_CODE_MAP", "C_CUSTOMER_CODE", "C_CUSTOMER_CODE_MAP", "WH_UPDATE_TIME", "op_type", "current_ts").show(10, truncate=False)

            hudi_options = result_hudi_options("T_BACK_ACCOUNT_MAP", "upsert")

            num_add = df_new_records_with_uuid.count()

            df_new_records_with_uuid.write \
            .format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(target_path)

            print(f"===> Added {num_add} new records to UUID table.")
        else:
            print("===> No new records to add.")
