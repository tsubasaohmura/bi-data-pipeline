from common_linux import clean_up, execute_hana_to_ch, get_clickhouse_client, get_hana_cursor, send_success, send_failure
from datetime import datetime, timedelta

# Specific function for current script.
def create_datelist(start, end):
    # Define startdate and enddate of list creation
    strdt = datetime.strptime(f"{start}", '%Y-%m-%d')  
    enddt = datetime.strptime(f"{end}", '%Y-%m-%d')  

    # Calculate date diff length
    days_num = (enddt - strdt).days + 1 

    # for loop
    datelist = []
    for i in range(days_num):
        dstr = str(strdt + timedelta(days=i))
        datelist.append(dstr[:4] + dstr[5:7] + dstr[8:10])
    return datelist

JOB_NAME = "hana_sales_detail_to_ch"
MAX_FETCH_ROWS = 500_000  # 100,000 rows ~= 650MB of memory
OUT_TABLE = "commercial.sales_detail"
#dates_list = create_datelist('2021-05-21', '2021-05-21')
dates_list = ['20210423', '20210521']


def main(date_yyyymmdd):
    sql_hana = f"""
SELECT *
FROM CCEJ_VIRTUAL."ccej.zj_sales.ccw.SalesReporting_Perf_Opt.data::persisted_sales_details.zj_sr_tr_sales_detail"
WHERE CALDAY = '{date_yyyymmdd}';
"""

    start_time = datetime.now()
    try:
        hana = get_hana_cursor()
        clickhouse = get_clickhouse_client()
        print(f"\nStarting daily sales job for day {date_yyyymmdd}.\n")
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