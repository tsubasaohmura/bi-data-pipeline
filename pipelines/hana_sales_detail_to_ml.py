# Prototype for extracting and uploading HANA daily sales data to MarkLogic.

from datetime import date, timedelta
from decimal import Decimal
from hdbcli import dbapi
import json
import os
from pathlib import Path
import re
import subprocess
from common import get_hana_cursor

# --- Constants
PARTITION_BY_NUM_FILES = 1000  # Max 100_000
MAX_FETCH_ROWS = 100_000  # 100,000 rows ~= 650MB of memory
#YESTERDAY_DATE = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
YESTERDAY_DATE = "20200901"
OUTPUT_PATH = Path.cwd().joinpath("data", "hana_daily_sales", YESTERDAY_DATE)
sql_query = f"""
SELECT * 
FROM CCEJ_VIRTUAL."ccej.zj_sales.ccw.SalesReporting_Perf_Opt.data::persisted_sales_details.zj_sr_tr_sales_detail"
WHERE CALDAY = {YESTERDAY_DATE}
LIMIT 15000
"""


# --- Functions
def extract_daily_sales_data(cursor, sql_query):
    print("Executing query to HANA...")
    cursor_response = cursor.execute(sql_query)
    if cursor_response is False:
        raise Exception("Cursor failed to execute SQL query.")
    
    record_number = 0
    file_number = 0

    while True:
        print(f"Fetching {MAX_FETCH_ROWS} rows...")
        result = cursor.fetchmany(MAX_FETCH_ROWS)
        if len(result) <= 0:
            print("Reached the end of query result set.")
            print(f"Total amount of rows: {record_number}")
            break
        else:
            print(f"Transforming and writing {len(result)} rows...")
            # Process amount of records defined by MAX_FETCH_ROWS. When exhausted, repeat loop and fetch more .
            while len(result) > 0:
                # Keep the length of one batch at 10, or less if the batch is smaller than 10 records
                length = 10 if len(result) > 10 else len(result)
                ten_records = [result.pop(0) for _ in range(length)]

                # Create a dictionary of 10 records
                sales_dict = {"sales": []}
                for record in ten_records:
                    record_number += 1
                    record_dict = {}
                    for column, value in zip(record.column_names, record.column_values):
                        if type(value) == Decimal: value = int(value)
                        # Data transformation
                        if value is None: value = ""
                        if column in ("CALMONTH") and value is not None: 
                            value = value[:4] + '-' + value[4:]
                        if column in ("CALDAY", "BUDAT", "BW_CALDAY_WEEK", "CALDAY_prevyear", "CALDAY_WEEK_prevyear") \
                        and value is not None:
                            value = value[:4] + '-' + value[4:6] + '-' + value[6:]
                        record_dict.update({column: value})
                    sales_dict["sales"].append(record_dict)
                
                # Output dictionary as JSON to disk
                final_directory = OUTPUT_PATH.joinpath(f"{file_number // PARTITION_BY_NUM_FILES}")
                final_directory.mkdir(parents=True, exist_ok=True)
                with open(final_directory.joinpath(f"{file_number}.json"), "w+", encoding="utf-8") as outfile:
                    json.dump(sales_dict, outfile, ensure_ascii=False)
                    file_number += 1
            print(f"Wrote {file_number}-th file to disk.")


def run_mlcp():
    # Get list sub-folders in OUTPUT_PATH
    partition_paths_list = [path for path in OUTPUT_PATH.glob("*/")]
    if not partition_paths_list: raise ValueError("Partition paths list is empty")

    for partition_path in partition_paths_list:
        partition_num = partition_path.stem
        print(f"Running command for this path: {partition_path.as_posix()}")
        print(f"Equals to this partition: {partition_num}")

        import_command = f"""C:\\Users\\kirill.denisenko\\mlcp-10.0.5\\bin\\mlcp.bat import ^
-ssl ^
-host 10.212.47.72 ^
-port 8000 ^
-username admin-user ^
-password admin ^
-database test-kirill ^
-input_file_path "{partition_path.as_posix()}/" ^
-input_file_type delimited_json ^
-generate_uri true ^
-output_uri_replace "^.*\/hana_daily_sales,'/hana_daily_sales',\.json-\d*-\d*,'.json'" ^
-output_collections "hana-daily-sales,hana-daily-sales-{YESTERDAY_DATE}-{partition_num}" ^
-output_permissions "read-role,read" ^
-thread_count 48
""".replace("\n","")
    
        command_result = subprocess.run(import_command, capture_output=True, universal_newlines=True, check=True)
        # Print out num of records using regex
        # group(0) means full regex match. group(1) means the value in ( ) only.
        total_paths = re.search("Total input paths to process : (\d+)", command_result.stderr).group(0)
        input_records = re.search("INPUT_RECORDS: (\d+)", command_result.stderr).group(0)
        output_records = re.search("OUTPUT_RECORDS: (\d+)", command_result.stderr).group(0)
        output_records_committed = re.search("OUTPUT_RECORDS_COMMITTED: (\d+)", command_result.stderr).group(0)
        re_output_records_failed = re.search("OUTPUT_RECORDS_FAILED: (\d+)", command_result.stderr)
        output_records_failed = re_output_records_failed.group(0)
        num_output_records_failed = int(re_output_records_failed.group(1))
        
        report = f"""
        Upload to MarkLogic completed.
        Date: {YESTERDAY_DATE}
        Partition: {partition_num}
        {total_paths}
        {input_records}
        {output_records}
        {output_records_committed}
        {output_records_failed}
        """

        print(report)

        if num_output_records_failed != 0:
            raise RuntimeError("Upload to MarkLogic unsuccessful. See details above.")

def main():
    # Step 1: Connect to HANA
    connection = get_hana_cursor()
    cursor = connection.cursor()
    
    # Step 2: Extract, save data and clear DB connection
    extract_daily_sales_data(cursor, sql_query)
    cursor.close()
    connection.close()
    
    # Step 3: Upload data to MarkLogic
    run_mlcp()

    # Step 4: Clean up used files
    # Will be done in the future
    print("All done.")

main()