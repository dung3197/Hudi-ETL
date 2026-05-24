from pyspark.sql.functions import udf, current_timestamp, from_utc_timestamp
import uuid
from src.configs.secret_manager import get_s3_conn
from src.helpers.hudi_utils import create_hudi_options
from src.configs.settings import settings
from src.helpers.hash_hudi_utils import create_hudi_options, create_session

app_name = "ETL BO RAW - HASH MKT"

def mkt_infor_warehouse_hash():
    s3_conn = get_s3_conn()
    spark = create_session(app_name, s3_conn)

    def result_hudi_options(source_table, write_operation) -> dict:
        return create_hudi_options("COPY_ON_WRITE", source_table, write_operation, "C_FRONT_USER_CODE", None, False, "C_FRONT_USER_CODE", "C_FRONT_USER_CODE")

    # ===================== #
    # 1. Đọc bảng gốc
    # ===================== #
    source_path = f"s3a://{settings.s3_source_mkt_path}"
    target_path = f"s3a://{settings.s3_dest_mkt_hash_path}"


    df_source = spark.read.format("hudi").load(source_path)
    print("|__________ Dropping NULL C_FRONT_USER_CODE...")
    df_source_drop_na = df_source.na.drop(subset=["C_FRONT_USER_CODE"])
    df_source_new = df_source_drop_na.select("C_FRONT_USER_CODE","op_type","current_ts")


    # ================================================= #
    # 2. Kiểm tra bảng đích đã tồn tại chưa
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
    # 3. UDF sinh UUID
    # ===================== #
    @udf("string")
    def gen_uuid():
        return str(uuid.uuid4().hex[:20])


    # ================================ #
    # 4. Xử lý lần đầu (init)
    # ================================ #
    if not table_exists == True:
        print("===> Initializing new UUID table with full clone...")
        
        init_update_time = from_utc_timestamp(current_timestamp(), "Asia/Ho_Chi_Minh")
        df_with_uuid = df_source_new.withColumn("C_FRONT_USER_CODE_MAP", gen_uuid()).withColumn("WH_UPDATE_TIME", init_update_time)
 
        hudi_options = result_hudi_options("T_LIST_FRONT_USER_MAP", "upsert")

        df_with_uuid.write \
            .format("hudi") \
            .options(**hudi_options) \
            .mode("overwrite") \
            .save(target_path)

    
    # ============================ #
    # 5. Incremental Update
    # ============================ #
    else:
        print("===> Incremental sync mode...")

        # Lấy C_FRONT_USER_CODE từ bảng đích
        print("|__________ Getting dest front user code list...")
        df_existing_ids = df_target.select("C_FRONT_USER_CODE").distinct()

        print("|__________ Getting newly added front user code list...")
        # Lọc record mới xuất hiện trong bảng gốc
        df_new_records = df_source_new.join(df_existing_ids, "C_FRONT_USER_CODE", "leftanti")
        print("|__________ Finished filtering...")

        if df_new_records.count() > 0:
            print("|_________________ Begin to generate UUID and update time...")
            incremental_update_time = from_utc_timestamp(current_timestamp(), "Asia/Ho_Chi_Minh")
            df_new_records_with_uuid = df_new_records.withColumn("C_FRONT_USER_CODE_MAP", gen_uuid()).withColumn("WH_UPDATE_TIME", incremental_update_time)
            df_new_records_with_uuid.select("C_FRONT_USER_CODE", "C_FRONT_USER_CODE_MAP", "WH_UPDATE_TIME", "op_type").show(10, truncate=False)

            hudi_options = result_hudi_options("T_LIST_FRONT_USER_MAP", "upsert")

            num_add = df_new_records_with_uuid.count()

            df_new_records_with_uuid.write \
            .format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(target_path)

            print(f"===> Added {num_add} new records to UUID table.")
        else:
            print("===> No new records to add.")

