from common import clean_up, send_success, send_failure, clean_up, get_clickhouse_client, execute_ar_to_ch
from datetime import datetime, timedelta

<<<<<<< HEAD:pipelines/hana_zjcust.py
JOB_NAME = "hana_zjcust Customer Master data"
MAX_FETCH_ROWS = 1_000_000
OUT_TABLE = "commercial.zjcust"

sql_hana = """
SELECT 
	"0REGION___T",
    "ZJCUST",
    "ZJBJICLV2___T",
    "ZJSREIGCE"
FROM "_SYS_BIC"."system-local.bw.bw2hana/ZJCUST"
"""
=======
JOB_NAME = "ar_to_ch"
MAX_FETCH_ROWS = 1_000_000  # 1,000,000 rows ~= 650MB of memory
OUT_TABLE = "ar_row"
OUT_TABLE_2 = "ar_transaction"
AR_TABLE = "ar_sample"
>>>>>>> d4212743fd72852a9e26c45f3af858e9d089de3b:pipelines/ar_to_ch.py

def main():
    start_time = datetime.now()
    try:
        clickhouse = get_clickhouse_client(environment="commercial")
        print(f"\nStarting AUGMENT ar job.\n")
        result = execute_ar_to_ch(
            clickhouse_client=clickhouse,
            ch_table=OUT_TABLE,
            ch_table_2 = OUT_TABLE_2,
            max_fetch_rows=MAX_FETCH_ROWS,
            ar_transform = True,
            truncate = True,
            ar_data = AR_TABLE
        )
        end_time = datetime.now()
        print(f"Elapsed time: {str(end_time - start_time)}")
        send_success(
            job=JOB_NAME, 
            rows=result["row_number"], 
            truncated=result["truncated"], 
            start_time=start_time, 
            end_time=end_time
            )
    except Exception as e:
        e_time = datetime.now()
        send_failure(
            job=JOB_NAME, 
            start_time=start_time, 
            e_time=e_time, 
            e=e
        )
        raise e
    finally:
        clean_up(clickhouse=clickhouse)    


if __name__ == "__main__":
    main()
