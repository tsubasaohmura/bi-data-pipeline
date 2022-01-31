from common import clean_up, send_success, send_failure, execute_sfsf_to_ch, get_clickhouse_client, get_sfsf_api_auth
from datetime import datetime, timedelta

JOB_NAME="sfsf_hr_to_ch"
OUT_TABLE="hr_master_test"


def main():
    start_time = datetime.now()
    try:
        sfsf = get_sfsf_api_auth()
        clickhouse = get_clickhouse_client(environment="commercial")
        print(f"\nStarting SFSF(REST API) job to genelate HRMaster.\n")
        result = execute_sfsf_to_ch(
            sfsf_auth=sfsf,
            clickhouse_client=clickhouse,
            ch_table=OUT_TABLE,
            last_mod_date=False, # When you debug, set value with 'yyyy-mm-dd' style.
            truncate=True
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
