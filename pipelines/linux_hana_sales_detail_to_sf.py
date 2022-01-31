from common_linux import clean_up, execute_hana_to_ch, get_clickhouse_client, get_hana_cursor, send_success, send_failure
from datetime import datetime

JOB_NAME = "hana_sales_detail_to_sf"
MAX_FETCH_ROWS = 500_000  # 100,000 rows ~= 650MB of memory
OUT_TABLE = "COMMERCIAL.DBP.SALES_DETAIL"
dates_list = [
    '20210301',
    '20210302'

    ]


def main(date_yyyymmdd):
    sql_clickhouse = f"""
SELECT *
FROM commercial.sales_detail
WHERE calday = '{date_yyyymmdd}';
"""

    start_time = datetime.now()
    try:
        clickhouse = get_clickhouse_client()
        snowflake = get_snowflake_client()
        print(f"\nStarting daily sales job for day {date_yyyymmdd}.\n")
        result = execute_hana_to_ch(
            clickhouse_client=clickhouse,
            snowflake_client=snowflake,
            sql_clickhouse=sql_hana,
            sf_table=OUT_TABLE,
            max_fetch_rows=MAX_FETCH_ROWS,
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
        clean_up(hana, clickhouse)

if __name__ == "__main__":
    for date_yyyymmdd in dates_list:
        main(date_yyyymmdd)