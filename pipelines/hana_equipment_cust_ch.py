from common import clean_up, send_success, send_failure, execute_hana_to_ch, get_hana_cursor, get_clickhouse_client
from datetime import datetime, timedelta

JOB_NAME = "hana_equipment_cust AR Customer Master data"
MAX_FETCH_ROWS = 1_000_000
OUT_TABLE = "equipment_cust"

sql_hana = f"""
SELECT
    "ZJCDSRECD",
    "ZJCDSRECD___T",
    "ZJLOCACD",
    "ZJLOCACD___T",
    "ZJSMWERKS",    
    "ZJSMPDROC"
FROM "_SYS_BIC"."system-local.bw.bw2hana/ZJCUST"
"""

def main():
    start_time = datetime.now()
    try:
        hana = get_hana_cursor(host='10.98.53.23')
        clickhouse = get_clickhouse_client(environment="commercial")
        result = execute_hana_to_ch(
            hana_cursor=hana,
            clickhouse_client=clickhouse,
            sql_hana=sql_hana,
            ch_table=OUT_TABLE,
            max_fetch_rows=MAX_FETCH_ROWS,
            truncate=True,
            equipment_cust=True
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
        clean_up(hana, clickhouse)

if __name__ == "__main__":
    main()
