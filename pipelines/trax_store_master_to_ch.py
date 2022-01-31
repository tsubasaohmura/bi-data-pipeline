from common import clean_up, send_success, send_failure, execute_trax_to_ch, get_clickhouse_client, get_trax_api_auth
from datetime import datetime, timedelta

JOB_NAME="TARX API Store Master Data"
TARGET_TABLE="store"
MAX_FETCH_ROWS=1_000_000
OUT_TABLE="trax_master_store"


def main():
    start_time = datetime.now()
    try:
        trax = get_trax_api_auth()
        clickhouse = get_clickhouse_client(environment="commercial")
        result = execute_trax_to_ch(
            clickhouse_client=clickhouse,
            ch_table=OUT_TABLE,
            max_fetch_rows=MAX_FETCH_ROWS,
            credentials=trax,
            truncate=True,
            master=TARGET_TABLE
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
