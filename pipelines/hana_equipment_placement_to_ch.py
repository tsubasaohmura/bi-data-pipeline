from common import clean_up, send_success, send_failure, execute_hana_to_ch, get_hana_cursor, get_clickhouse_client
from datetime import datetime, timedelta

JOB_NAME = "hana_equipment_to_ch"
MAX_FETCH_ROWS = 100_000  # 100,000 rows ~= 650MB of memory
OUT_TABLE = "equipment_test"

dates_list = [
    '20200904',
    '20200905'
 ]

#today = datetime.strftime(datetime.today(), '%Y%m%d')
#strdt = datetime.strptime("20200201", '%Y%m%d') 
#enddt = datetime.strptime("20200630", '%Y%m%d') 
#days_num = (enddt - strdt).days + 1
#dates_list = []
#for i in range(days_num):
#    dates_list.append(str(strdt + timedelta(days=i))[:4]
#     + str(strdt + timedelta(days=i))[5:7] 
#     + str(strdt + timedelta(days=i))[8:10])

def main(date_yyyymmdd):
    sql_hana = f"""
SELECT * 
FROM _SYS_BIC."ccbji.zj_eqdb.eqdb_placement/ZJ_CV_PLACEMENT"
WHERE ADDAT = {date_yyyymmdd}
"""

    start_time = datetime.now()
    try:
        hana = get_hana_cursor(host='10.98.53.23')
        clickhouse = get_clickhouse_client(environment='tsubasa_test')
        print(f"\nStarting equipment(placement) job for day {date_yyyymmdd}.\n")
        result = execute_hana_to_ch(
            hana_cursor=hana,
            clickhouse_client=clickhouse,
            sql_hana=sql_hana,
            ch_table=OUT_TABLE,
            max_fetch_rows=MAX_FETCH_ROWS,
            equipment_transform=True
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
