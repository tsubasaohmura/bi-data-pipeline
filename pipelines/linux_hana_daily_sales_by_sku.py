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

def get_query(day: str, month: str, filename: str)->str:
    with open(os.path.join("home/appsetup/airflow/sql", filename), "r") as f:
        return f.read().format(calday=day, calmonth=month)


# Define arguments
JOB_NAME = "hana_daily_sales_ej_to_ch"
MAX_FETCH_ROWS = 500_000  # 100,000 rows ~= 650MB of memory
OUT_TABLE = "commercial.hana_transaction_daily_sales_by_sku"
dates_list = create_datelist('2021-11-04', '2021-11-04')


def main(date_yyyymmdd):
    sql_hana = f"""
SELECT DISTINCT
  "BW_CALDAY_WEEK", 
  "0CALDAY_WEEK_prevyear", 
  "ZJDIVGCD" AS EIGYO_HONBU_CODE,
  "ZJDIVGCD___T" AS EIGYO_HONBU,
  "ZJDIVCD" AS CHIKU_HONBU_CODE,
  "ZJDIVCD___T" AS CHIKU_HONBU,
  "ZJRGNCD" AS BRANCH_CODE,  
  "ZJRGNCD___T" AS BRANCH_NAME,
  "ZJBJICLV2" AS channel_l2_code,
  "ZJBJICLV2___T" AS channel_l2_name,
  "ZJBJICL_M", 
  "ZJBJICL_M___T", 
  "ZJANCD",
  COALESCE(SUM("CM_CY_BAPC"),0) AS BAPC_TY,
  COALESCE(SUM("CM_LY_BAPC"),0) AS BAPC_LY, 
  COALESCE(SUM("CM_BAPC_VS_LY_DIFF_CL"),0) AS BAPC_vs_LY, 
  COALESCE(SUM("CM_BAPC_VS_LY_PERCENT_CL"),0) AS BAPC_vs_LY_percentage, 
  COALESCE(SUM("CM_CY_GP_BAPC"),0) AS GP_BAPC, 
  COALESCE(SUM("CM_CY_NSR"),0)  AS NSR_TY, 
  COALESCE(SUM("CM_LY_NSR"),0)  AS NSR_LY, 
  COALESCE(SUM("CM_NSR_VS_LY_DIFF_CL"),0) AS NSR_vs_LY, 
  COALESCE(SUM("CM_NSR_VS_LY_PERCENT_CL"),0) AS NSR_vs_LY_percentage, 
  COALESCE(SUM("CM_CY_GP"),0)   AS GP_TY, 
  COALESCE(SUM("CM_LY_GP"),0)   AS GP_LY
FROM
  "_SYS_BIC"."ccej.zj_sales.ccw.SalesReporting_Perf_Opt/ZJ_CCBJI_SR_GENERAL_REPORT" (  
    'PLACEHOLDER' = ('$$pMonth$$', '''{date_yyyymmdd[:4] + date_yyyymmdd[4:6]}'''), 
    'PLACEHOLDER' = ( 
      '$$pBudgetFlag$$', 
      '''X:バジェット対象(飲料製品)'',''Y:バジェット対象(飲料以外・KO-EAST/FV)'''),
    'PLACEHOLDER' = ('$$pDataFilterFlag$$', '''0:通常'''), 
    'PLACEHOLDER' = ( 
      '$$pOldBotCd2$$', 
      ''''',''13'',''21'',''22'',''23'',''24'',''28'',''32'',''39'''), 
    'PLACEHOLDER' = ('$$pDDISExclusionFlag$$', '''送信対象''')
  ) 
WHERE BW_CALDAY_WEEK = '{date_yyyymmdd}'
GROUP BY
  "BW_CALDAY_WEEK", "0CALDAY_WEEK_prevyear", "ZJDIVGCD", 
  "ZJDIVCD", "ZJBJICL_M", "ZJDIVGCD___T", 
  "ZJDIVCD___T", "ZJBJICL_M___T", "ZJBJICLV2", "ZJBJICLV2___T",
  "ZJRGNCD", "ZJRGNCD___T", "ZJANCD"
UNION ALL
SELECT DISTINCT
  "BW_CALDAY_WEEK", 
  "0CALDAY_WEEK_prevyear", 
  "ZJDIVGCD" AS EIGYO_HONBU_CODE,
  "ZJDIVGCD___T" AS EIGYO_HONBU,
  "ZJDIVCD" AS CHIKU_HONBU_CODE,
  "ZJDIVCD___T" AS CHIKU_HONBU,
  "ZJRGNCD" AS BRANCH_CODE,  
  "ZJRGNCD___T" AS BRANCH_NAME,
  "ZJBJICLV2" AS channel_l2_code,
  "ZJBJICLV2___T" AS channel_l2_name,
  "ZJBJICL_M", 
  "ZJBJICL_M___T", 
  "ZJANCD",
  COALESCE(SUM("CM_CY_BAPC"),0) AS BAPC_TY,
  COALESCE(SUM("CM_LY_BAPC"),0) AS BAPC_LY, 
  COALESCE(SUM("CM_BAPC_VS_LY_DIFF_CL"),0) AS BAPC_vs_LY, 
  COALESCE(SUM("CM_BAPC_VS_LY_PERCENT_CL"),0) AS BAPC_vs_LY_percentage, 
  COALESCE(SUM("CM_CY_GP_BAPC"),0) AS GP_BAPC, 
  COALESCE(SUM("CM_CY_NSR"),0)  AS NSR_TY, 
  COALESCE(SUM("CM_LY_NSR"),0)  AS NSR_LY, 
  COALESCE(SUM("CM_NSR_VS_LY_DIFF_CL"),0) AS NSR_vs_LY, 
  COALESCE(SUM("CM_NSR_VS_LY_PERCENT_CL"),0) AS NSR_vs_LY_percentage, 
  COALESCE(SUM("CM_CY_GP"),0)   AS GP_TY, 
  COALESCE(SUM("CM_LY_GP"),0)   AS GP_LY
FROM
  "_SYS_BIC"."ccej.zj_sales.ccw.SalesReporting_Perf_Opt/ZJ_CCBJI_SR_GENERAL_REPORT" ( 
    'PLACEHOLDER' = ('$$pMonth$$', '''{date_yyyymmdd[:4] + date_yyyymmdd[4:6]}'''), 
    'PLACEHOLDER' = ('$$pBudgetFlag$$', '''X:バジェット対象(飲料製品)'''), 
    'PLACEHOLDER' = ('$$pDataFilterFlag$$', '''0:通常'''), 
    'PLACEHOLDER' = ('$$pOldBotCd2$$', '''35'',''42'''), 
    'PLACEHOLDER' = ('$$pDDISExclusionFlag$$', '''送信対象''')
  ) 
WHERE BW_CALDAY_WEEK = '{date_yyyymmdd}'
GROUP BY
  "BW_CALDAY_WEEK", "0CALDAY_WEEK_prevyear", "ZJDIVGCD", 
  "ZJDIVCD", "ZJBJICL_M", "ZJDIVGCD___T", 
  "ZJDIVCD___T", "ZJBJICL_M___T", "ZJBJICLV2", "ZJBJICLV2___T",
  "ZJRGNCD", "ZJRGNCD___T", "ZJANCD"
UNION ALL  
SELECT DISTINCT
  "BW_CALDAY_WEEK", 
  "0CALDAY_WEEK_prevyear", 
  "ZJDIVGCD" AS EIGYO_HONBU_CODE,
  "ZJDIVGCD___T" AS EIGYO_HONBU,
  "ZJDIVCD" AS CHIKU_HONBU_CODE,
  "ZJDIVCD___T" AS CHIKU_HONBU,
  "ZJRGNCD" AS BRANCH_CODE,  
  "ZJRGNCD___T" AS BRANCH_NAME,
  "ZJBJICLV2" AS channel_l2_code,
  "ZJBJICLV2___T" AS channel_l2_name,
  "ZJBJICL_M", 
  "ZJBJICL_M___T", 
  "ZJANCD",
  COALESCE(SUM("CM_CY_BAPC"),0) AS BAPC_TY,
  COALESCE(SUM("CM_LY_BAPC"),0) AS BAPC_LY, 
  COALESCE(SUM("CM_BAPC_VS_LY_DIFF_CL"),0) AS BAPC_vs_LY, 
  COALESCE(SUM("CM_BAPC_VS_LY_PERCENT_CL"),0) AS BAPC_vs_LY_percentage, 
  COALESCE(SUM("CM_CY_GP_BAPC"),0) AS GP_BAPC, 
  COALESCE(SUM("CM_CY_NSR"),0)  AS NSR_TY, 
  COALESCE(SUM("CM_LY_NSR"),0)  AS NSR_LY, 
  COALESCE(SUM("CM_NSR_VS_LY_DIFF_CL"),0) AS NSR_vs_LY, 
  COALESCE(SUM("CM_NSR_VS_LY_PERCENT_CL"),0) AS NSR_vs_LY_percentage, 
  COALESCE(SUM("CM_CY_GP"),0)   AS GP_TY, 
  COALESCE(SUM("CM_LY_GP"),0)   AS GP_LY
FROM
  "_SYS_BIC"."ccej.zj_sales.ccw.SalesReporting_Perf_Opt/ZJ_CCBJI_SR_GENERAL_REPORT" ( 
    'PLACEHOLDER' = ('$$pMonth$$', '''{date_yyyymmdd[:4] + date_yyyymmdd[4:6]}'''), 
    'PLACEHOLDER' = ('$$pCompany$$', '''7828'''), 
    'PLACEHOLDER' = ( 
      '$$pBudgetFlag$$', 
      '''X:バジェット対象(飲料製品)'',''Y:バジェット対象(飲料以外・KO-EAST/FV)'',''Z:バジェット対象(飲料以外・FVのみ)'''
    ) , 
    'PLACEHOLDER' = ('$$pDataFilterFlag$$', '''2: Vending_OCS (223)'',''3: 営業本部外'''), 
    'PLACEHOLDER' = ('$$pDDISExclusionFlag$$', '''送信対象''')
  ) 
WHERE BW_CALDAY_WEEK = '{date_yyyymmdd}'
GROUP BY
  "BW_CALDAY_WEEK", "0CALDAY_WEEK_prevyear", "ZJDIVGCD", 
  "ZJDIVCD", "ZJBJICL_M", "ZJDIVGCD___T", 
  "ZJDIVCD___T", "ZJBJICL_M___T", "ZJBJICLV2", "ZJBJICLV2___T",
  "ZJRGNCD", "ZJRGNCD___T", "ZJANCD"
ORDER BY
  "BW_CALDAY_WEEK", "0CALDAY_WEEK_prevyear","EIGYO_HONBU", 
  "CHIKU_HONBU", "BRANCH_NAME", "ZJBJICL_M", "ZJANCD"
;
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
#    except Exception as e:
#        e_time = datetime.now()
#        send_failure(
#            job=JOB_NAME, 
#            start_time=start_time, 
#            e_time=e_time, 
#            e=e
#        )
#        raise e
    finally:
        clean_up(hana, clickhouse)

if __name__ == "__main__":
    for date_yyyymmdd in dates_list:
        main(date_yyyymmdd)
