from common import clean_up, execute_hana_to_ch, get_clickhouse_client, get_hana_cursor, send_success, send_failure
from datetime import datetime

JOB_NAME = "hana_sales_detail_to_ch"
MAX_FETCH_ROWS = 1_000  # 100,000 rows ~= 650MB of memory
OUT_TABLE = "tsubasa_test.zjmate"
HOST="10.212.40.188"


def main():
    sql_hana = f"""
SELECT
    "ZJANCD" AS zjancd,
    "ZJBEVMNC___T" AS zjbevmnc___t,
    "ZJCATCD___T" AS zjcatcd___t,
    "ZJDDCSCD___T" AS zjddcscd___t,
    "ZJDDCTCD___T" AS zjddctcd___t,
    "ZJEJBRAND___T" AS zjejbrand___t,
    "ZJMATE" AS zjmate,
    "ZJMATE___T" AS zjmate___t,
    "ZJNEWPRDC___T" AS zjnewprdc___t,
    "ZJPACKCD___T" AS zjpackcd___t,
    "ZJRELDT" AS zjreldt,
    "ZJRPPPKG___T" AS zjrpppkg___t,
    "ZJSBCATCD___T" AS zjsbcatcd___t,
    "ZJSUBCATC___T" AS zjsubcatc___t,
    "ZJBUDTF" AS zjbudtf
FROM "_SYS_BIC"."system-local.bw.bw2hana/ZJMATE"

"""


    start_time = datetime.now()
    try:
        hana = get_hana_cursor()
        clickhouse = get_clickhouse_client('tsubasa_test', HOST)
        print(f"\nStarting daily sales job for day.\n")
        result = execute_hana_to_ch(
            hana_cursor=hana,
            clickhouse_client=clickhouse,
            sql_hana=sql_hana,
            ch_table=OUT_TABLE,
            max_fetch_rows=MAX_FETCH_ROWS,
            daily_sales_transform=True
        )
        end_time = datetime.now()
        print(f"Elapsed time: {str(end_time - start_time)}")
        send_success(
            job=JOB_NAME, 
            rows=result["row_number"], 
            truncated=result["truncated"], 
            start_time=start_time, 
            end_time=end_time, 
            date=date_yyyymmdd
            )
    #except Exception as e:
    #    e_time = datetime.now()
    #    send_failure(
    #        job=JOB_NAME, 
    #        start_time=start_time, 
    #        e_time=e_time, 
    #        e=e
    #    )
    #    raise e
    finally:
        clean_up(hana, clickhouse)

if __name__ == "__main__":
    main()
