import logging
import codecs
import pendulum
from pathlib import Path

from airflow.hooks.clickhouse_hook import ClickHouseHook
from airflow.models.variable import Variable
from airflow.settings import AIRFLOW_HOME
from airflow.utils.email import send_email

from plugins.hooks.sap_hana_hook import SapHanaHook

from numpy import nan
from pandas import DataFrame, merge, json_normalize, to_datetime
from datetime import timedelta, date, datetime, timezone
import calendar
from decimal import Decimal
import json
import os
import requests
from requests.auth import HTTPBasicAuth


# Temporarily enable proxy from Linux server to the Internet. 
os.environ['http_proxy']="http://10.139.60.231:8080"
os.environ['https_proxy']="https://10.139.60.231:8080"

LOCAL_TIMEZONE='Asia/Tokyo'

### Processing functions

def hana_to_ch(*, hana_conn_id, hana_sql_file, ch_conn_id, ch_target_table, transform_func=None, truncate=False,
               transaction=False, sales_detail=False, bp=False, monthly_job=False, target_date='21990101',
               daily_sales=False, mtd=False, max_fetch_rows=100_000,  **context):
    """Extract data from HANA database and insert into target table in ClickHouse.
    Support table truncation and additional data transformation passed as a function.
    Uses ONLY KEYWORD ARGUMENTS.
        
    Args:
        hana_conn_id (str): Connection string to HANA database defined in Airflow UI.
        hana_sql_file (str): name of the file containing SELECT SQL query to HANA.
            Must be located in `sql` folder.
        ch_conn_id (str): Connection string to ClickHouse database defined in Airflow UI.
        ch_target_table (str): name of the target table. Recommended to use `database.table` syntax.
        transform_func (function): function object for data transformation.
            Applied to every row extracted from HANA and before inserting into ClickHouse.
            Default: None.
        sales_detail: Special function for sales_detail data.
        target_date: Used for transaction data execution.
        truncate (bool): whether to truncate ClickHouse target table before inserting data. Default: False.
        max_fetch_rows (int): how many rows to process at a time. Default: 100_000.
    Returns:
        dict:
            row_number: number of rows processed.
            truncate: whether ClickHouse target table was truncated or not.
    """
    logger = logging.getLogger("airflow.task")
    
    if not callable(transform_func) and transform_func is not None:
        raise Exception("Object passed to transform_func is not a function.")

    if transaction:
        # Create second target_date for sales_detail
        target_date2 = str(datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=1))[:10]
        # Create target month
        target_month = target_date[:4] + target_date[5:7]
        current_month_first = target_date[:4] + "-" + target_date[5:7] + "-01"
        target_month_previous = str(datetime.strptime(current_month_first, '%Y-%m-%d') - timedelta(days=1))[:7]
        target_month_previous_hana = target_month_previous[:4] + target_month_previous[5:7] 
        # Define first date and last date of month.
        def get_last_date(dt):
            return dt.replace(day=calendar.monthrange(dt.year, dt.month)[1])
        target_date_first = target_date[:4] + "-" + target_date[5:7] + "-01"
        target_date_last = str(get_last_date(date.today()))
        # Remove "-" from target_date for SQL qurerying to HANA.
        target_date_hana = target_date[:4] + target_date[5:7] + target_date[8:]# Define target date (only year/month).
        target_date2_hana = target_date2[:4] + target_date2[5:7] + target_date2[8:]# Define target date (only year/month).
        target_date_first_hana = target_date_first[:4] + target_date_first[5:7] + target_date_first[8:]

    def get_query(day: str, day_first: str, month: str, filename: str)->str:
        with open(os.path.join("home/appsetup/airflow/sql", filename), "r") as f:
            return f.read().format(calday=day, calday_first=day_first, calmonth=month)
    
    if sales_detail:
        hana_sql = f"""
SELECT *
FROM CCEJ_VIRTUAL."ccej.zj_sales.ccw.SalesReporting_Perf_Opt.data::persisted_sales_details.zj_sr_tr_sales_detail"
WHERE CALDAY BETWEEN '{target_date2_hana}' AND '{target_date_hana}';
"""
    elif monthly_job:
        hana_sql = f"""
SELECT *
FROM CCEJ_VIRTUAL."ccej.zj_sales.ccw.SalesReporting_Perf_Opt.data::persisted_sales_details.zj_sr_tr_sales_detail"
WHERE CALMONTH = '{target_month}';
"""
    elif bp:
        hana_sql = f"""
SELECT
    "0CALDAY" AS "CALDAY",
    "ZJDATE__ZJCPDATE",
    "ZJBJIARA1" AS "営業本部コード",
    "ZJBJIARA2" AS "地区本部コード",
    "ZJBJICL_M" AS "CCBJI Channel Lv.3コード",
    sum("ZJHANSU_P") AS "販売数量BP", 
    sum("ZJBJINSRP") AS "実売上高BP", 
    sum("ZJGROSS_P") AS "売上総利益BP",
    sum("ZJBJIMP_P") AS "限界利益BP"
FROM "_SYS_BIC"."system-local.bw.bw2hana/ZJBJD002"
WHERE "0CALMONTH" = '{target_month}'
GROUP BY
    "0CALDAY",
    "ZJDATE__ZJCPDATE",
    "ZJBJIARA1",
    "ZJBJIARA2",
    "ZJBJICL_M"
ORDER BY
    "0CALDAY",
    "ZJDATE__ZJCPDATE",
    "ZJBJIARA1",
    "ZJBJIARA2",
    "ZJBJICL_M"
"""
    elif daily_sales:
        hana_sql = f"""
{get_query(day=target_date_hana, day_first=target_date_first_hana, month=target_month, filename=hana_sql_file)}
"""
    else:
        hana_sql_file = Path(AIRFLOW_HOME) / 'sql' / hana_sql_file
        with codecs.open(hana_sql_file, 'r', 'utf-8') as t:
            hana_sql = t.read()

    # Safety check. We can only SELECT from HANA.
    if any(word in hana_sql.upper() for word in ['ALTER', 'DELETE', 'UPDATE', 'TRUNCATE']) \
        and 'SELECT' not in hana_sql.upper():
        raise Exception('HANA SQL must be a SELECT query.')
    
    # Database hooks
    hana = SapHanaHook(hana_conn_id)
    ch = ClickHouseHook(ch_conn_id)
    
    try:    
        table_truncated = False        
        row_number = 0
        
        hana_cursor = hana.query(hana_sql)

        # Remove current month's bp data first to refresh.
        if bp or mtd:
            logger.info(f"Refreshing ClickHouse table {ch_target_table} current month only...")
            ch.run(f"ALTER TABLE {ch_target_table} DELETE WHERE calday BETWEEN '{target_date_first}' AND '{target_date_last}';")
            logger.info(f"Refreshed current month's {ch_target_table} table successfuly.")
            print(hana_sql)

        # Remove day before yesterday's data to adjust GMT/JST timediff between C1 and HANA
        if sales_detail:
            # Remove day before yesterday's data
            logger.info(f"Refreshing ClickHouse table {ch_target_table} day before yesterday ({target_date2}) only...")
            ch.run(f"ALTER TABLE {ch_target_table} DELETE WHERE calday = '{target_date2}';")
            logger.info(f"Refreshed day before yesterday's {ch_target_table} table successfuly.")
            # Remove yesterday's LY data.
            logger.info(f"Refreshing ClickHouse table {ch_target_table} yesterday LY record ({target_date}) only...")
            ch.run(f"ALTER TABLE {ch_target_table} DELETE WHERE calday = '{target_date}';")
            logger.info(f"Refreshed yesterday's {ch_target_table} table successfuly.")

            logger.info(f"Getting ready for sales_detail job.")

        while True:
            logger.info(f"Fetching {row_number}~ rows from HANA...")
            result = hana_cursor.fetchmany(max_fetch_rows)
            if len(result) == 0 and row_number == 0: raise Exception("Failed to fetch any data.")
            
            if truncate and not table_truncated:
                logger.info(f"Truncating ClickHouse table {ch_target_table}...")
                ch.run(f"TRUNCATE TABLE {ch_target_table}")
                table_truncated = True
            
            # Transform data if transform function is passed
            if transform_func:
                logger.info(f"Transforming data using {transform_func.__name__} .")
                payload = transform_func(result)
            if transaction:
                if sales_detail:
                    logger.info(f'Extracting {target_date2} to {target_date} HANA transaction data.')
                if monthly_job:
                    logger.info(f'Extracting whole {target_month} HANA transaction data.')
                else:
                    logger.info(f'Extracting {target_date} HANA transaction data.')
                payload = []
                for record in result:
                    transformed_record = []
                    for column, value in zip(record.column_names, record.column_values):
                        if column in ("CALMONTH") and value is not None: 
                            value = value[:4] + '-' + value[4:]
                        if column in ("CALDAY", "BUDAT", "BW_CALDAY_WEEK", "CALDAY_prevyear", "CALDAY_WEEK_prevyear", "0CALDAY", "ZJDATE__ZJCPDATE", "0CALDAY_WEEK_prevyear") \
                        and value is not None:
                            value = datetime.strptime(value, "%Y%m%d").date()
                        if daily_sales:
                            if value is None:
                                value = ''
                        if isinstance(value, Decimal):
                            value = str(value)
                        transformed_record.append(value)
                    assert len(transformed_record) == len(record)
                    payload.append(tuple(transformed_record))
                logger.info('Sales Detail / BP table has been transformed.')
            else:
                payload = result
            logger.info(f"Inserting {len(payload)} rows into ClickHouse {ch_target_table}.")
            ch.run(f"INSERT INTO {ch_target_table} VALUES", payload)
            row_number += len(payload)
            if len(payload) < max_fetch_rows:
                logger.info("Reached the end of query result set.")
                logger.info(f"Total number of rows: {row_number}")
                break
        
        return {"row_number": row_number, "truncated": table_truncated}

    finally:
        hana_cursor.connection.close()

 
def run_mlcp(*, input_dir, output_dir, collections, conn_id, data_format='json', **context):
    """Run MarkLogic Content Pump via Shell command and upload data to MarkLogic.
    Uses ONLY KEYWORD ARGUMENTS.
    """
   

def trax_to_ch(ch_conn_id, ch_table, ch_table2=False, ch_table3=False, ch_table4=False, ch_table5=False, ch_table6=False, ch_table7=False, ch_table8=False, ch_table9=False,
               master=False, transaction=False, execute_date='2199-01-01', truncate=False, max_fetch_rows=1_000_000, **context):
    """Extract data from HANA database and insert into target table in ClickHouse.
    Support table truncation and additional data transformation passed as a function.
    Uses ONLY KEYWORD ARGUMENTS.
        
    Args:
        ch_conn_id (str): Connection string to ClickHouse database defined in Airflow UI.
        ch_table (str): name of the target table. Recommended to use `database.table` syntax.
        ch_table2 (str): name of the target table. Recommended to use `database.table` syntax.
        master(str):
        transaction(bool): Through Bulk Analysis JSON extraction, generate 5 all transactional tables.
        execute_date(str): Define target date of Bulk Analysis JSON records. Only used for transaction mode.
        truncate (bool): whether to truncate ClickHouse target table before inserting data. Default: False.
        max_fetch_rows (int): how many rows to process at a time. Default: 100_000.
    Returns:
        dict:
            row_number: number of rows processed.
            truncate: whether ClickHouse target table was truncated or not.
    """
    logger = logging.getLogger("airflow.task")
    
    # Database hooks
    ch = ClickHouseHook(ch_conn_id)
    trax_auth = get_trax_api_auth()

    try:    
        table_truncated = False        
        row_number = 0

        # Define arguments
        ch_truncate_sql = f"TRUNCATE TABLE {ch_table}"    
        trax_project = trax_auth['project']
        trax_api_key = trax_auth['apikey']
        
        if truncate and not table_truncated:
            logger.info(f"Truncating ClickHouse table {ch_table}...")
            ch.run(f"TRUNCATE TABLE {ch_table}")
            table_truncated = True
        
        # Master data creation
        if master:
            link_list = [] #list for put session links
            # Define specific arguments
            if master == 'store':
                entity_name = 'store?sort=store_number'
                # TRAX CUSTOMER MASTER
                url_trax_master_customer = "https://services.traxretail.com/api/v4/ccjp/entity/store?sort=store_number&page=1&per_page=500"
                cols_master_customer = ['Customer code', 'Store name', 'Store display name', 'store type name', 'street',
                                        'address line2','City', 'postal code', 'latitude', 'longitude', 'manager name',
                                        'manager phone', 'manager email', 'region name', 'district name', 'branch name',
                                        'retailer name', 'state code', 'store_additional_attribute_1', 'Account code', 'Account name', 'Office code', 'Office name',
                                        'Route code', 'person in charge', 'person in charge mail', 'Channel', 'Segment',
                                        'Prefecture', 'is_active',
                                        'additional_attribute8', 'additional_attribute11', 'additional_attribute12',
                                        'additional_attribute14', 'additional_attribute15', 'additional_attribute16',
                                        'additional_attribute17', 'additional_attribute18', 'additional_attribute19',
                                        'additional_attribute20', 'additional_attribute21', 'additional_attribute22',]
                table_master = DataFrame(index = [], columns = cols_master_customer)

                sprint_link = 1
                while True: #loop to get every session links at specific date duration
                #for i in range(50): #for debug
                    logger.info(f'sprint{sprint_link}')
                    response_trax_master_customer_list = requests.get(url_trax_master_customer, headers={"Authorization":f"{trax_api_key}"}) #GET request to TRAX
                    result_trax_master_customer_list = json.loads(response_trax_master_customer_list.text)
                    for link in range(len(result_trax_master_customer_list["store"])): #loop to get 200 session links into "link_list"
                        place = result_trax_master_customer_list["store"][link]
                        link_list.append({'Customer code' : place["store_number"],
                                                        'Store name' : place["store_name"],
                                                        'Store display name' : place["store_display_name"],
                                                        'store type name':place['store_type_name'],
                                                        'street':place['street'],
                                                        'address line2':place['address_line_2'],
                                                        'postal code':place['postal_code'],
                                                        'latitude':place['latitude'],
                                                        'longitude':place['longitude'],
                                                        'manager name':place['manager_name'],
                                                        'manager phone':place['manager_phone'],
                                                        'manager email':place['manager_email'],
                                                        'region name' : place["region_name"],
                                                        'district name': place["district_name"],
                                                        'branch name': place["branch_name"],
                                                        'retailer name': place["retailer_name"],
                                                        'state code': place["state_code"],
                                                        'store_additional_attribute_1':place["store_additional_attributes"]["additional_attribute_1"] if "additional_attribute_1" in place["store_additional_attributes"] else nan,
                                                        'Account code' : place["store_additional_attributes"]["additional_attribute_4"] if "additional_attribute_4" in place["store_additional_attributes"] else nan, 
                                                        'Account name' : place["store_additional_attributes"]["additional_attribute_3"] if "additional_attribute_3" in place["store_additional_attributes"] else nan, 
                                                        'Office code' : place["store_additional_attributes"]["additional_attribute_5"] if "additional_attribute_5" in place["store_additional_attributes"] else nan,
                                                        'Office name' : place["store_additional_attributes"]["additional_attribute_6"] if "additional_attribute_6" in place["store_additional_attributes"] else nan,
                                                        'Route code' : place["store_additional_attributes"]["additional_attribute_7"] if "additional_attribute_7" in place["store_additional_attributes"] else nan,
                                                        'person in charge' : place["store_additional_attributes"]["additional_attribute_9"] if "additional_attribute_9" in place["store_additional_attributes"] else nan,
                                                        'person in charge mail' : place["store_additional_attributes"]["additional_attribute_13"] if "additional_attribute_13" in place["store_additional_attributes"] else nan,
                                                        'Channel' : place["store_additional_attributes"]["additional_attribute_10"] if  "additional_attribute_10" in place["store_additional_attributes"] else nan, 
                                                        'Segment' : "blank", 'City' : place["city"], 'Prefecture' : place["district_name"],
                                                        'is_active':place["is_active"],
                                                        'additional_attribute8': place["store_additional_attributes"]["additional_attribute_8"] if "additional_attribute_8" in place["store_additional_attributes"] else nan,
                                                        'additional_attribute11': place["store_additional_attributes"]["additional_attribute_11"] if "additional_attribute_11" in place["store_additional_attributes"] else nan,
                                                        'additional_attribute12': place["store_additional_attributes"]["additional_attribute_12"] if "additional_attribute_12" in place["store_additional_attributes"] else nan,
                                                        'additional_attribute14': place["store_additional_attributes"]["additional_attribute_14"] if "additional_attribute_14" in place["store_additional_attributes"] else nan,
                                                        'additional_attribute15': place["store_additional_attributes"]["additional_attribute_15"] if "additional_attribute_15" in place["store_additional_attributes"] else nan,
                                                        'additional_attribute16': place["store_additional_attributes"]["additional_attribute_16"] if "additional_attribute_16" in place["store_additional_attributes"] else nan,
                                                        'additional_attribute17': place["store_additional_attributes"]["additional_attribute_17"] if "additional_attribute_17" in place["store_additional_attributes"] else nan,
                                                        'additional_attribute18': place["store_additional_attributes"]["additional_attribute_18"] if "additional_attribute_18" in place["store_additional_attributes"] else nan,
                                                        'additional_attribute19': place["store_additional_attributes"]["additional_attribute_19"] if "additional_attribute_19" in place["store_additional_attributes"] else nan,
                                                        'additional_attribute20': place["store_additional_attributes"]["additional_attribute_20"] if "additional_attribute_20" in place["store_additional_attributes"] else nan,
                                                        'additional_attribute21': place["store_additional_attributes"]["additional_attribute_21"] if "additional_attribute_21" in place["store_additional_attributes"] else nan,
                                                        'additional_attribute22': place["store_additional_attributes"]["additional_attribute_22"] if "additional_attribute_22" in place["store_additional_attributes"] else nan,})
                    url_trax_master_customer = "https://services.traxretail.com" + result_trax_master_customer_list["metadata"]["links"]["next"] #over write the "url_trax_list"
                    if result_trax_master_customer_list["metadata"]["links"]["next"] == "": #when the page was the last page
                        break
                    sprint_link += 1
            if master == 'product':
                entity_name = 'product?sort=product_name'
                # TRAX PRODUCT MASTER
                url_trax = f"https://services.traxretail.com/api/v4/ccjp/entity/{entity_name}&page=0&per_page=500"
                cols_master = ['product_code', 'product_name', 'product_type', 'product_uuid', 'product_local_name',
                                    'product_short_name', 'brand_name', 'brand_local_name', 'manufacturer_name', 'manufacturer_local_name',
                                    'is_deleted', 'product_client_code','container_type', 'size', 'unit_measurement',
                                    'is_active', 'category_name', 'category_local_name', 'sub_category_name', 'sub_category_local_name', 'images']
                table_master = DataFrame(index = [], columns = cols_master)

                sprint_link = 1
                while True: #loop to get every session links at specific date duration
                    logger.info(f'sprint{sprint_link}')
                    response_trax_list = requests.get(url_trax, headers={"Authorization":f"{trax_api_key}"}) #GET request to TRAX
                    result_trax_list = json.loads(response_trax_list.text)
                    for link in range(len(result_trax_list["product"])): #loop to get 200 session links into "link_list"
                        place = result_trax_list["product"][link]
                        link_list.append({'product_code': place['pk'], 'product_name': place['product_name'], 'product_type': place['product_type'],
                                          'product_uuid': place['product_uuid'], 'product_local_name': place['product_local_name'],
                                          'product_short_name': place['product_short_name'], 'brand_name': place['brand_name'],
                                          'brand_local_name': place['brand_local_name'], 'manufacturer_name': place['manufacturer_name'],
                                          'manufacturer_local_name': place['manufacturer_local_name'],'is_deleted': place['is_deleted'],
                                          'product_client_code': place['product_client_code'] if 'product_client_code' in place else nan, 
                                          'container_type': place['container_type'] if 'container_type' in place else nan,
                                          'size': place['size'] if 'size' in place else nan, 'unit_measurement': place['unit_measurement'] if 'unit_measurement' in place else nan,
                                          'is_active': place['is_active'], 'category_name': place['category_name'], 'category_local_name': place['category_local_name'],
                                          'sub_category_name': place['subcategory_name'] if 'subcategory_name' in place else nan,
                                          'sub_category_local_name': place['subcategory_local_name'] if 'subcategory_local_name' in place else nan,
                                          'images': place['images'][0]['image_url'] if 'images' in place else nan
                                          })
                    url_trax = "https://services.traxretail.com" + result_trax_list["metadata"]["links"]["next"] #over write the "url_trax_list"
                    if result_trax_list["metadata"]["links"]["next"] == "": #when the page was the last page
                        break
                    sprint_link += 1
            
            # DataFrame normalization & consolidation.
            df_master = json_normalize(link_list)# Change to DataFrame
            table_master = table_master.append(df_master).astype(str)
            # Special filteration for store master
            if master == 'store':
                logger.info('Special filteration for store master.')
                table_master = table_master[(table_master['City'] == 'Tokyo') | (table_master['City'] == 'Non_Tokyo')].astype(str).fillna('Null')

            # Contain into payload.
            payload = table_master.values.tolist() # Transform DF to list

        # Transaction data creation
        if transaction:
            logger.info(f'Starting to extract {execute_date} transactions.')
            # Define arguments.
            start_date=execute_date
            end_date=execute_date
            cols_poc = ['Session id', 'Email', 'Visit date','Start time',
            'End time', 'Visit type', 'GPS latitude', 'GPS longitude',
            'Store latitude', 'Store longitude','Customer code', 'POC Type', 
            'location', 'count']
            cols_poc_score = ['Session id', 'Email', 'Visit date','Start time',
                            'End time', 'Visit type', 'GPS latitude', 'GPS longitude',
                            'Store latitude', 'Store longitude','Customer code', 'Availabillity count', 
                            'Target', 'Priority POC score', 'Priority POC score *weight']
            cols_msl_store = ['Session id', 'Email', 'Visit date','Start time',
                            'End time', 'Visit type', 'GPS latitude', 'GPS longitude',
                            'Store latitude', 'Store longitude','Customer code', 'ActualMSL', 
                            'ExpectedMSL', 'MSL%', 'MSL% *weight']
            cols_msl_product = ['Session id', 'Email', 'Visit date','Start time',
                            'End time', 'Visit type', 'GPS latitude', 'GPS longitude',
                            'Store latitude', 'Store longitude','Customer code', 'Product Code', 
                            'Availlability', 'MSL Type']
            cols_sovi_category = ['Session id', 'Email', 'Visit date','Start time',
                            'End time', 'Visit type', 'GPS latitude', 'GPS longitude',
                            'Store latitude', 'Store longitude', 'Customer code', 'Manufacturer', 'Category', 
                            'Actual Facing', 'Total Facing', 'SOVI']
            cols_sovi_store = ['Session id', 'Email', 'Visit date','Start time',
                            'End time', 'Visit type', 'GPS latitude', 'GPS longitude',
                            'Store latitude', 'Store longitude', 'Customer code', 'Manufacturer', 
                            'Actual Facing', 'Total Facing', 'SOVI']
            cols_unique = ['Session id', 'Email', 'Visit date','Start time',
                            'End time', 'Visit type', 'GPS latitude', 'GPS longitude',
                            'Store latitude', 'Store longitude', 'Customer code', 'Manufacturer', 
                            'Unique Product Count', 'Total Product Count', 'Unique SOVI']
            cols_red = ['Session id', 'Email', 'Visit date','Start time',
                        'End time', 'Visit type', 'GPS latitude', 'GPS longitude',
                        'Store latitude', 'Store longitude', 'Customer code', 'Red Score']
            cols_photo = ['scene_id', 'scene_uid', 'scene_status', 'scene_closed_by_TTL',
                          'store_area_code', 'task_name', 'task_display_name', 'task_code',
                          'task_uuid', 'thumbnail_url', 'preview_url', 'image_uid', 'capture_time', 'customer_code',
                          'image_url_original','image_url_medium', 'image_url_small']

            table_poc = DataFrame(index = [], columns = cols_poc)
            table_poc_score = DataFrame(index = [], columns = cols_poc_score)
            table_msl_store = DataFrame(index = [], columns = cols_msl_store)
            table_msl_product = DataFrame(index = [], columns = cols_msl_product)
            table_sovi_category = DataFrame(index = [], columns = cols_sovi_category)
            table_sovi_store = DataFrame(index = [], columns = cols_sovi_store)
            table_unique = DataFrame(index = [], columns = cols_unique)
            table_red = DataFrame(index = [], columns = cols_red)
            table_photo = DataFrame(index = [], columns = cols_photo)

            # Extract all active BD's specified link.
            logger.info("Bulk Analysis data extraction 1 \Extract all active BD's specified link.")
            url_trax_list = f"https://services.traxretail.com/api/v4/ccjp/analysis-results?from_visit_date={start_date}&to_visit_date={end_date}"
            link_list = [] # list for put session links
            while True: # loop to get every session links at specific date duration
                response_trax_list = requests.get(url_trax_list, headers={"Authorization":f"{trax_api_key}"}) #GET request to TRAX
                result_trax_list = json.loads(response_trax_list.text)
                if 'results' in result_trax_list:       
                    for link in range(len(result_trax_list["results"])): # loop to get 200 session links into "link_list"
                        link_list.append(result_trax_list["results"][link]["results_link"])
                    url_trax_list = "https://services.traxretail.com" + result_trax_list["metadata"]["links"]["next"] #over write the "url_trax_list"
                    if result_trax_list["metadata"]["links"]["next"] == "": # When the page was the last page, 
                        break
                else:
                    logger.info("Today's record was zero. Script will be ended without execute anything." )
                    break

            #link_list = link_list[0:100] # \\\---For debuging only.---///

            # Generate actual records in each BD's.
            logger.info("Bulk Analysis data extraction 2 \Generate actual records in each BD's.")
            logger.info("This takes for a while to proceed.")
            sprint = 1
            for link in link_list: #loop by every session links to get whole poc transactonal data in sepecific date dulation
                logger.info(f'sprint{sprint}')
                url_trax_unique = link
                response_trax = requests.get(url_trax_unique, headers={"Authorization":f"{trax_api_key}"}) #GET request to TRAX
                result_trax = json.loads(response_trax.text)
                
                # Define arguments
                json_collections = {'Session id' : result_trax['session_uid'], 'Email' : result_trax['visitor_identifier'],
                                'Visit date' : result_trax['session_date'], 'Start time': result_trax['session_start_time'],
                                'End time': result_trax['session_end_time'], 'Visit type': result_trax['visit_type_name'],
                                'GPS latitude': result_trax['GPS_coordinates_latitude'], 'GPS longitude': result_trax['GPS_coordinates_longitude'],
                                'Store latitude': result_trax['store_GPS_coordinates_latitude'], 'Store longitude': result_trax['store_GPS_coordinates_longitude'],
                                'Customer code' : result_trax['store_number']}

                length = len(result_trax['details']['kpis']) # Define length for following loop
                length_photo = len(result_trax['details']['images'])
                for num in range(length): # loop to take POC data from "kpis" entity
                    # Define key feature.
                    place = result_trax['details']['kpis'][num] #variable
                    json_collections_poc = dict(**json_collections, **{'POC Type': place['entities'][0]['uid'],
                                                                'location': place['entities'][1]['uid'],
                                                                'count': place['numerator']})
                    json_collections_poc_score = dict(**json_collections, **{'Availabillity count': place['numerator'],
                                                                    'Target': place['denominator'],
                                                                    'Priority POC score': place['result'], 
                                                                    'Priority POC score *weight': place['score']})
                    json_collections_msl_store = dict(**json_collections, **{'ActualMSL': place['numerator'],
                                                                    'ExpectedMSL': place['denominator'],
                                                                    'MSL%': place['result'], 
                                                                    'MSL% *weight': place['score']})
                    json_collections_unique = dict(**json_collections, **{'Manufacturer': place['entities'][0]['uid'],
                                                                    'Unique Product Count': place['numerator'],
                                                                    'Total Product Count': place['denominator'],
                                                                    'Unique SOVI': place['result']})
                    json_collections_red = dict(**json_collections, **{'Red Score': place['result']})

                    # Define list as column.
                    poc_list = [json_collections_poc] # Dict list for "CCJP_POC_COUNT_BY_TASK"
                    poc_score_list = [json_collections_poc_score] # For "CCJP_POC_SCORE_BY_TARGET"
                    msl_store_list = [json_collections_msl_store] # For "CCJP_Dst_Manufacturer_in_Whole_Store"
                    unique_list = [json_collections_unique] # For "CCJP_UNIQUE_DIST_OWN_MANU"
                    red_list = [json_collections_red] # For "CCJP_RED_SCORE"

                    # Generate POC table
                    if result_trax['details']['kpis'][num]['name'] == 'CCJP_POC_COUNT_BY_TASK':
                        poc_list.append(json_collections_poc)
                        df_poc = json_normalize(poc_list).drop(0, axis=0).reset_index(drop=True)# Change to DataFrame and drop dummy row
                        table_poc = table_poc.append(df_poc)
                    # Generate POC score table
                    if result_trax['details']['kpis'][num]['name'] == 'CCJP_POC_SCORE_BY_TARGET':
                        poc_score_list.append(json_collections_poc_score)
                        df_poc_score = json_normalize(poc_score_list).drop(0, axis=0).reset_index(drop=True)# Change to DataFrame and drop dummy row
                        table_poc_score = table_poc_score.append(df_poc_score)
                    # Generate MSL store table
                    if result_trax['details']['kpis'][num]['name'] == 'CCJP_Dst_Manufacturer_in_Whole_Store':
                        msl_store_list.append(json_collections_msl_store)
                        df_msl_store = json_normalize(msl_store_list).drop(0, axis=0).reset_index(drop=True)# Change to DataFrame and drop dummy row
                        table_msl_store = table_msl_store.append(df_msl_store)
                    # Generate MSL product table
                    # MSL product
                    if place['name'] == 'CCJP_Dst_Manufacturer_in_Whole_Store':
                        place_child = place['results']
                        child_len = len(place_child)# Define length for child KPIs loop.
                        for nums in range(child_len):
                            json_collections_msl_product = dict(**json_collections, **{'Product Code': place_child[nums]['entities'][0]['uid'],
                                                            'Availlability': place_child[nums]['numerator'], # Define collections here.
                                                            'MSL Type': place_child[nums]['result']})
                            msl_product_list = [json_collections_msl_product] # For "CCJP_Product_Presence_in_Whole_Store"
                            if place_child[nums]['name'] == 'CCJP_Product_Presence_in_Whole_Store':
                                msl_product_list.append(json_collections_msl_product)
                                df_msl_product = json_normalize(msl_product_list).drop(0, axis=0).reset_index(drop=True)# Change to DataFrame and drop dummy row
                                table_msl_product = table_msl_product.append(df_msl_product)
                    # SOVI Category
                    if place['name'] == 'CCJP_FSOS_Own_Manufacturer_By_Category':
                        place_child = place['results']
                        child_len = len(place_child)# Define length for child KPIs loop.
                        for nums in range(child_len):
                            json_collections_sovi_category = dict(**json_collections, **{'Manufacturer': place_child[nums]['entities'][0]['uid'],
                                                                            'Category': place_child[nums]['entities'][1]['uid'],
                                                                            'Actual Facing': place_child[nums]['numerator'], 
                                                                            'Total Facing': place_child[nums]['denominator'],
                                                                            'SOVI': place_child[nums]['result']})
                            sovi_category_list = [json_collections_sovi_category] # For "CCJP_FSOS_All_Manufacturer_By_Category"
                            if place_child[nums]['name'] == 'CCJP_FSOS_All_Manufacturer_By_Category':
                                sovi_category_list.append(json_collections_sovi_category)
                                df_sovi_category = json_normalize(sovi_category_list).drop(0, axis=0).reset_index(drop=True)# Change to DataFrame and drop dummy row
                                table_sovi_category = table_sovi_category.append(df_sovi_category)
                    # SOVI Store
                    if place['name'] == 'CCJP_FSOS_Own_Manufacturer_In_Whole_Store':
                        place_child = place['results']
                        child_len = len(place_child)# Define length for child KPIs loop.
                        for nums in range(child_len):
                            json_collections_sovi_store = dict(**json_collections, **{'Manufacturer': place_child[nums]['entities'][0]['uid'],
                                                                            'Actual Facing': place_child[nums]['numerator'], 
                                                                            'Total Facing': place_child[nums]['denominator'],
                                                                            'SOVI': place_child[nums]['result']})
                            sovi_store_list = [json_collections_sovi_store] # For "CCJP_FSOS_All_Manufacturer_in_Whole_Store"
                            if place_child[nums]['name'] == 'CCJP_FSOS_All_Manufacturer_In_Whole_Store':
                                sovi_store_list.append(json_collections_sovi_store)
                                df_sovi_store = json_normalize(sovi_store_list).drop(0, axis=0).reset_index(drop=True)# Change to DataFrame and drop dummy row
                                table_sovi_store = table_sovi_store.append(df_sovi_store)
                    if place['name'] == 'CCJP_UNIQUE_DIST_OWN_MANU':
                        unique_list.append(json_collections_unique)
                        df_unique = json_normalize(unique_list).drop(0, axis=0).reset_index(drop=True)# Change to DataFrame and drop dummy row
                        table_unique = table_unique.append(df_unique)
                    if place['name'] == 'CCJP_RED_SCORE':
                        red_list.append(json_collections_red)
                        df_red = json_normalize(red_list).drop(0, axis=0).reset_index(drop=True)# Change to DataFrame and drop dummy row
                        table_red = table_red.append(df_red)
                    else:
                        None
                # Loop for photos    
                for num in range(length_photo): # loop to take photo sessions
                    # Define key feature.
                    place = result_trax['details']['images'][num] #variable
                    json_collections_photo = {'scene_id':place['scene_id'], 'scene_uid':place['scene_uid'], 'scene_status':place['scene_status'],
                                            'scene_closed_by_TTL':place['scene_closed_by_TTL'], 'store_area_code':place['store_area_code'],
                                            'task_name':place['task_name'], 'task_display_name':place['task_display_name'], 
                                            'task_code':place['task_code'], 'task_uuid':place['task_uuid'], 'thumbnail_url':place['thumbnail_url'],
                                            'preview_url':place['preview_url']}
                    for nums in range(len(place['scene_images'])):
                        places = place['scene_images'][nums]
                        json_collections_photos = dict(**json_collections_photo, **{'image_uid':places['image_uid'], 'capture_time':places['capture_time'],
                                                                                    'customer_code':result_trax['store_number'],                                            
                                                                                    'image_url_original':places['image_urls']['original'],
                                                                                    'image_url_medium':places['image_urls']['medium'],
                                                                                    'image_url_small':places['image_urls']['small']})
                        # Define list as column.
                        photo_list = [json_collections_photos] # Dict list for "CCJP_POC_COUNT_BY_TASK"
                        photo_list.append(json_collections_photos)
                        df_photo = json_normalize(photo_list).drop(0, axis=0).reset_index(drop=True)# Change to DataFrame and drop dummy row
                        table_photo = table_photo.append(df_photo)                       
                sprint += 1 
            table_poc = table_poc.reset_index(drop=True)
            table_poc_score = table_poc_score.reset_index(drop=True)
            table_msl_store = table_msl_store.reset_index(drop=True)
            table_msl_product = table_msl_product.reset_index(drop=True)
            table_sovi_category = table_sovi_category.reset_index(drop=True)
            table_sovi_store = table_sovi_store.reset_index(drop=True)
            table_unique = table_unique.reset_index(drop=True)
            table_red = table_red.reset_index(drop=True)
            table_photo = table_photo.reset_index(drop=True)

            # Data format section.
            logger.info("Bulk Analysis data extraction 3 \Data format section.")

            def trans_common_columns_datatype(dataframe):
                df = dataframe
                df = df.astype({'Session id':str, 'Email':str, 'Visit type':str, # Change data type in right format
                                'Customer code':str, 'GPS latitude':str, 'GPS longitude':str, 'Store latitude':str, 'Store longitude':str})
                return df
            table_poc = trans_common_columns_datatype(table_poc)
            table_poc = table_poc.astype({'count':int})
            table_poc_score = trans_common_columns_datatype(table_poc_score)
            table_poc_score = table_poc_score.astype({'Availabillity count':int, 'Target':int,
                                                      'Priority POC score':float, 'Priority POC score *weight':float})
            table_msl_store = trans_common_columns_datatype(table_msl_store)
            table_msl_store = table_msl_store.astype({'ActualMSL':int, 'ExpectedMSL':int,
                                                      'MSL%':float, 'MSL% *weight':float})
            table_msl_product = trans_common_columns_datatype(table_msl_product)
            table_msl_product = table_msl_product.astype({'Product Code':str, 'Availlability':int,
                                                    'MSL Type':str})
            table_sovi_category = trans_common_columns_datatype(table_sovi_category)
            table_sovi_category = table_sovi_category.astype({'Manufacturer':str, 'Category':str, 'Actual Facing':int,
                                                    'Total Facing':int, 'SOVI':float})
            table_sovi_store = trans_common_columns_datatype(table_sovi_store)
            table_sovi_store = table_sovi_store.astype({'Manufacturer':str, 'Actual Facing':int,
                                                    'Total Facing':int, 'SOVI':float})
            table_unique = trans_common_columns_datatype(table_unique)
            table_unique = table_unique.astype({'Manufacturer':str, 'Unique Product Count':int,
                                                'Total Product Count':int, 'Unique SOVI':float})
            table_red = trans_common_columns_datatype(table_red)
            table_red = table_red.astype({'Red Score':float})
            table_photo = table_photo.astype(str).astype({'capture_time':int})

            def trans_unixtime_to_datetime(dataframe, photo=False):
                df = dataframe
                if photo:
                    for times in range(len(df)):
                        epoc = df['capture_time'][times]
                        if not str(epoc).isdecimal():
                            continue
                        true_time = datetime.fromtimestamp(epoc, timezone(timedelta(hours=+9), 'JST')).strftime('%Y/%m/%d %H:%M:%S')
                        df['capture_time'][times] = true_time
                    df['capture_time'] = to_datetime(df['capture_time'])
                else:    
                    for times in range(len(df)):
                        epoc_start = df['Start time'][times]
                        epoc_end = df['End time'][times]
                        if not str(epoc_start).isdecimal():
                            continue
                        if not str(epoc_end).isdecimal():
                            continue  
                        true_start = datetime.fromtimestamp(epoc_start, timezone(timedelta(hours=+9), 'JST')).strftime('%Y/%m/%d %H:%M:%S')
                        true_end = datetime.fromtimestamp(epoc_end, timezone(timedelta(hours=+9), 'JST')).strftime('%Y/%m/%d %H:%M:%S')
                        df['Start time'][times] = true_start
                        df['End time'][times] = true_end
                    df['Start time'] = to_datetime(df['Start time'])
                    df['End time'] = to_datetime(df['End time'])
                    df['Visit date'] = to_datetime(df['Visit date'])
                return df
            table_poc = trans_unixtime_to_datetime(table_poc)
            table_poc_score = trans_unixtime_to_datetime(table_poc_score)
            table_msl_store = trans_unixtime_to_datetime(table_msl_store)
            table_msl_product = trans_unixtime_to_datetime(table_msl_product)
            table_sovi_category = trans_unixtime_to_datetime(table_sovi_category)
            table_sovi_store = trans_unixtime_to_datetime(table_sovi_store)
            table_unique = trans_unixtime_to_datetime(table_unique)
            table_red = trans_unixtime_to_datetime(table_red)
            table_photo = trans_unixtime_to_datetime(table_photo, photo=True)

            payload = table_poc.values.tolist() # Finally, transform into list type. 
            payload2 = table_poc_score.values.tolist()    
            payload3 = table_msl_store.values.tolist()
            payload4 = table_msl_product.values.tolist()
            payload5 = table_sovi_category.values.tolist()  
            payload6 = table_sovi_store.values.tolist()  
            payload7 = table_unique.values.tolist()
            payload8 = table_red.values.tolist()
            payload9 = table_photo.values.tolist()

        if not master and not transaction:
            logger.info("Please check the prametor")
            raise Exception('Please define "master" argument if you want to execute program.')

        # Execution section
        def insert_query(payloads, target_table):
            inserting_sql = f"INSERT INTO {target_table} VALUES"
            logger.info(f"Inserting {len(payloads)} rows into ClickHouse {target_table}.")
            ch.run(inserting_sql, payloads)
        insert_query(payload, ch_table)
        if ch_table2:
            insert_query(payload2, ch_table2)
            insert_query(payload3, ch_table3)
            insert_query(payload4, ch_table4)
            insert_query(payload5, ch_table5)
            insert_query(payload6, ch_table6)
            insert_query(payload7, ch_table7)
            insert_query(payload8, ch_table8)
            insert_query(payload9, ch_table9)
            row_number = [len(payload), len(payload2), len(payload3), len(payload4), len(payload5),
                          len(payload6), len(payload7), len(payload8), len(payload9)]
        else:
            row_number = [len(payload)]
        for i in range(len(row_number)):
            if row_number[i] > max_fetch_rows:
                logger.info("Reached the end of query result set.")
                logger.info(f"Total number of trax_rows: {row_number}")
                raise Exception('You have to redefine max fetch rows.')
            else:
                logger.info("Row number is much smaller than max fetch rows and don't have to consider RAM spaces.")
        return {"row_number": row_number, "truncated": table_truncated}

    finally:
        logger.info('All transform process has been done. Otsukaresamadeshita!')


def sfsf_to_ch(ch_conn_id, ch_table, last_mod_date=None, truncate=False, **context):
    """Request GET API call to SFSF Sydney data center and insert into ClickHouse.
    Args:
        sfsf_auth(dict): Try to authorise and return id, pass for API request. It will come from `get_sfsf_api_auth` function.
        clickhouse (clickhouse_driver.client.Client): instance of ClickHouse client
        ch_table (str): destination table in ClickHouse.
        max_fetch_rows (int): how many rows to fetch from HANA per one batch.
        truncate (bool): whether to truncate ClickHouse table before inserting.
    Returns:
        dict:
            row_number (int): total number of ingested rows
            truncated (bool): whether ch_table was truncated or not
    """
    logger = logging.getLogger("airflow.task")

    # Database hooks $ authorization
    ch = ClickHouseHook(ch_conn_id)
    sfsf_auth = get_sfsf_api_auth()


    table_truncated = False
    row_number = 0
    user = sfsf_auth["userid"]
    company = sfsf_auth["companyid"]
    password = sfsf_auth["password"]
    emp_status = "&$filter=emplStatus eq '1987' or emplStatus eq '1991' or emplStatus eq '1996'" # Filter to avaid containing retiree.
    filtered = ""
    if last_mod_date:
        filtered = f"&$filter=lastModifiedDateTime gt datetimeoffset'{last_mod_date}T00:00:00Z'" # Must to be set as DateTime format.
    if truncate and not table_truncated:
        logger.info(f"Truncating ClickHouse table {ch_table}...")
        #clickhouse_client.execute(ch_truncate_sql)
        ch.run(f"TRUNCATE TABLE {ch_table}")
        table_truncated = True
    
    logger.info("Start to send GET requests to SFSF...")
    # Define table
    entity_list = {"FODepartment" : {"entity":"FODepartment",
                                    "columns":['name_localized', 'parent', 'externalCode',
                                                'cust_string17','name', 'customString1', 'customString8'],
                                    "filter":""
                                },
                "PerPersonal": {"entity":"PerPersonal",
                                "columns":['personIdExternal', 'startDate', 'endDate','nationality', 'gender',
                                            'customString5', 'customString6', 'customString4', 'lastName', 
                                            'middleName', 'firstName', 'maritalStatus', 'challengeStatus'],
                                "filter":""
                        }, 
                "EmpJob": {"entity":"EmpJob",
                        "columns":['userId', 'managerId', 'company', 'jobCode', 'department', 'jobTitle',
                                            'customString2','customString25', 'customString24'],
                        "filter":"&$filter=emplStatus eq '1987' or emplStatus eq '1991' or emplStatus eq '1996'"
                        }, 
                "PerEmail": {"entity":"PerEmail",
                            "columns":['personIdExternal', 'emailAddress']
                        }
                    }
    def call_api(entity_name):
        # Variables
        entity = entity_name["entity"]
        columns = entity_name["columns"]
        output_table = DataFrame(index=[], columns=columns)

        #top = f"&$top=1000"
        skips = 0
        while True:
            #skip = f"&$skip={skips}"
            logger.info(f'The pipeline got {int(skips+1000)} rows from {entity}')
            url = f"https://api10.successfactors.com/odata/v2/{entity}?$format=JSON&$fromDate=2009-12-30{filtered}&$top=1000&$skip={skips}"
            if entity == "EmpJob":
                url = f"https://api10.successfactors.com/odata/v2/{entity}?$format=JSON&$fromDate=2009-12-30{emp_status}&$top=1000&$skip={skips}"
            response = requests.get(url, auth=HTTPBasicAuth(user+"@"+company, password)) 
            result = response.json()
            df = json_normalize(result["d"]["results"]) # transform to dataframe 
            df_drop = df.loc[:, columns]
            output_table = output_table.append(df_drop)
            skips += 1000
            if len(df) < 1000: # SFSF API maximum responce length is limited in 1000. Less than 1000 means it is bottom of rows.
                logger.info(f'Now, rows are just {len(df)} and that means it is the bottom of table.')
                break

        logger.info(f'Finally, the pipeline requests {int(skips/1000)} times')

        if entity == "FODepartment":
            output_table = output_table.dropna(subset=['customString8']) # drop na row and keep latest sections only
            output_table = output_table.rename(columns={'name': 'SectionNameJP', 'customString1': 'SectionNameEN',
                                                        'customString8': 'LCD','externalCode': 'SectionCD','parent': 'ParentSectionCD'
                                                        })
            output_table = output_table[output_table.SectionCD.str.match('[0-9]{10}')] # drop old organization by section code
        return output_table

    table_department = call_api(entity_list["FODepartment"])
    table_name = call_api(entity_list["PerPersonal"])
    table_mail = call_api(entity_list["PerEmail"])
    table_job = call_api(entity_list["EmpJob"])


    ## hierarchical master creation
    logger.info('Creating hierarchical master...')
    cols_hierarchy = ['L0Code', 'L0NameEN', 'L0NameJP', 'L1Code', 'L1NameEN', 'L1NameJP', 'L2Code', 'L2NameEN', 'L2NameJP',
                    'L2.5Code', 'L2.5NameEN', 'L2.5NameJP', 'L3Code', 'L3NameEN', 'L3NameJP', 'L4_1Code', 'L4_1NameEN', 'L4_1NameJP',
                    'L4_2Code', 'L4_2NameEN', 'L4_2NameJP', 'L4_3Code', 'L4_3NameEN', 'L4_3NameJP', 'L4_4Code',
                    'L4_4NameEN', 'L4_4NameJP', 'L5_1Code',  'L5_1NameEN', 'L5_1NameJP', 'L5_2Code', 'L5_2NameEN', 'L5_2NameJP',
                    'L6Code', 'L6NameEN', 'L6NameJP', 'identifier'
                    ]
    table_hierarchy = DataFrame(index = [], columns = cols_hierarchy)
    all_dept = table_department[["LCD", "ParentSectionCD", "SectionCD"]]
    merged_dept = all_dept.rename(columns={'SectionCD': 'SectionCD_0', 'LCD': 'LCD_0',
                                            'ParentSectionCD': 'ParentSectionCD_0',
                                            })

    i = 0
    while True:
        to_merge_dept = all_dept.rename(columns={'SectionCD': f'SectionCD_{i+1}','LCD': f'LCD_{i+1}',
                                                    'ParentSectionCD': f'ParentSectionCD_{i+1}'
                                                    })
        merged_dept = merge(merged_dept, to_merge_dept,
            how='left',
            left_on=f'ParentSectionCD_{i}',
            right_on=f'SectionCD_{i+1}',
            sort=False,
            indicator=True,
            )
        end_check = len(merged_dept['_merge'].unique()) == 1 and merged_dept['_merge'].unique()[0] == 'left_only'
        merged_dept.drop(columns='_merge', inplace=True)
        if end_check:
            cols = [
                f'SectionCD_{i+1}',
                f'LCD_{i+1}',
                f'ParentSectionCD_{i+1}',
            ]
            merged_dept.drop(columns=cols, inplace=True)
            break
        i += 1

    # add values to right place
    logger.info('Genelating HR Master')
    for l in range(len(merged_dept)):
        output = merged_dept[l:l+1].reset_index()
        output["Identifier"] = "dummy"
        key = str(output.at[0, "SectionCD_0"])
        for i in range(len(merged_dept.columns)//3):
            cnt = i
            column_value = str(output.at[0, f"LCD_{cnt}"])
            if column_value != 'nan':
                column_value = column_value.replace("-", "_")
                output = output.rename(columns = {f'LCD_{i}': f'{column_value}NameJP',
                                    f'ParentSectionCD_{i}': f'{column_value}NameEN',
                                    f'SectionCD_{i}': f'{column_value}Code'})
                identifier = output.at[0 ,f"{column_value}Code"]
                if identifier is None:
                    break
                elif identifier:
                    index = table_department[table_department.SectionCD == identifier].reset_index()
                    output[f"{column_value}NameEN"] = index[index.SectionCD == identifier].at[0, "SectionNameEN"]
                    output[f"{column_value}NameJP"] = index[index.SectionCD == identifier].at[0, "name_localized"]
                    output.Identifier = key
                else:
                    continue
        table_hierarchy = table_hierarchy.append(output.dropna(how='any', axis=1))
    table_hierarchy = table_hierarchy.drop(["index"], axis=1)
    table_hierarchy = table_hierarchy.rename(columns={"L2.5Code":"L2_5Code", "L2.5NameEN":"L2_5NameEN","L2.5NameJP":"L2_5NameJP"})

    # merge 4 entities into one table        
    master_table = merge(table_name, table_mail, left_on='personIdExternal',
                        right_on='personIdExternal', how='outer') # merge PerPersonal and PerEmail by outer merging
    master_table = merge(master_table, table_job, left_on='personIdExternal',
                        right_on='userId') # merge EmpJob and drop lefted person
    master_table = merge(master_table, table_department, left_on='department', right_on='SectionCD')
    master_table = master_table.drop(["ParentSectionCD", "cust_string17", "SectionNameJP", "startDate", "endDate"], axis=1)
    master_table = master_table[~master_table.duplicated(subset='personIdExternal')] # drop duplicated rows
    master_table = merge(master_table, table_hierarchy, left_on="SectionCD", right_on="Identifier")
    master_table = master_table.drop(['personIdExternal', 'Identifier', 'challengeStatus'], axis=1) # drop unnesessary columns
    master_table = master_table.rename(columns={'customString5': 'LastNameJP', 'customString6': 'MiddleNameJP','customString4': 'FirstNameJP',
                                                'lastName': 'LastNameEN','middleName': 'MiddleNameEN', 'firstName': 'FirstNameEN',
                                            'maritalStatus': 'Status', 'emailAddress': 'Email', 'userId': 'EmployeeCode',
                                            'managerId': 'ManagerEmployeeCode', 'company': 'CompanyCode','jobCode': 'JobCode', 
                                            'department': 'DepartmentCode', 'jobTitle': 'JobTitleEN', 'customString2': 'JobTitleJP',
                                            'customString24': 'JobTitleCommonJP', 'customString25': 'JobTitleCommonEN',
                                            'nationality': 'Nationality', 'gender': 'Gender', 'name_localized' : 'SectionNameJP', 'LCD' : 'LCode'
                                            })
    # clean up LCD NAME JP column
    clean_list = ["0", "1", "2", "2_5", "3", "4_1", "4_2", "4_3", "4_4"]
    for number in clean_list:
        col = f"L{number}NameJP"
        master_table[col] = master_table[col].replace('\w+[0-9]{1}', nan, regex=True) # remove "L0" figure from values
    master_table = master_table.fillna(" ") # fill NaN value for ClickHouse inserting

    payload = master_table.values.tolist()  # Transform DF to list
    logger.info(f"Inserting {len(payload)} rows into ClickHouse {ch_table}.")
    #clickhouse_client.execute(ch_insert_sql, payload)
    ch.run(f"INSERT INTO {ch_table} VALUES", payload)
    row_number += len(payload)
    logger.info(f"\nTotal number of CCBJI group employees: {row_number}")
            
    
    return {"row_number": row_number, "truncated": table_truncated}



### Utility functions

def get_sfsf_api_auth():
    """Connect to SFSF database and get responce by API"""
    logger = logging.getLogger("airflow.task")

    # Credentials
    userid = Variable.get('sfsf_user')
    companyid = Variable.get('sfsf_company')
    passwordsfsf = Variable.get('sfsf_pass')

    url = "https://api10.successfactors.com/odata/v2"
    response_auth = requests.get(url, auth=HTTPBasicAuth(userid+"@"+companyid, passwordsfsf)) 
    if not response_auth.ok:
        raise ConnectionError(f"Connection to SFSF database unsuccessful. Please check your credentials and settings. {userid}, {companyid}, {passwordsfsf}")

    logger.info('connection successful')
    return {"userid" : userid, "companyid" : companyid, "password" : passwordsfsf}


def get_trax_api_auth():
    """Connect to SFSF database and get responce by API"""
    logger = logging.getLogger("airflow.task")

    # Credentials
    project = Variable.get('trax_project')
    apikey = Variable.get('trax_apikey')

    url = f"https://services.traxretail.com/api/v4/{project}/entity/store?sort=store_number&page=2"
    response_auth = requests.get(url, headers={"Authorization":f"{apikey}"}) #GET request to TRAX 
    if not response_auth.ok:
        raise ConnectionError(f"Connection to TRAX database unsuccessful. Please check your credentials and settings. {project}, {apikey}")
    logger.info('TRAX API connection successful')
    return {"project" : project, "apikey" : apikey}


def send_job_success(transaction_date=False, external=False, **context):
    """Send a successful job execution email to email address(es) 
    defined in `notification_emails` variable in Airflow UI.
    Args:
        None    
    Returns:
        None
    """
    c = context
    subject = f"[SUCCESS] { c['ds'] } { c['dag'].dag_id }"
    # Adjust Pendulum date to Airflow's schedule logic
    start_time = c['execution_date'].in_timezone(LOCAL_TIMEZONE)
    end_time = pendulum.now(tz=LOCAL_TIMEZONE)
    # Account for correct date if DAG was triggered manually
    if (end_time - start_time).in_days() == 1:
        start_time = start_time.add(days=1)
    # 
    if transaction_date:
        target_date_indicator=transaction_date
    else:
        target_date_indicator="This pipeline is not for transactional data."
    last_task_return_value = c['ti'].xcom_pull(key="return_value")
    parameters = {
        'DagRun ID': c['run_id'],
        'Processed date': c['ds'],
        'Total amount of rows': last_task_return_value.get('row_number', "N/A") ,
        'Destination table truncated?': last_task_return_value.get('truncated', "N/A"),
        'Target date': target_date_indicator,
        'Scheduled start time (JST)': start_time.to_datetime_string(),
        'Actual end time (JST)': end_time.to_datetime_string(),
        'Elapsed time': str(end_time - start_time),
    }

    html_msg = f"""<table>
    { ''.join(f'<tr><td>{key}:</td><td>{value}</td></tr>' for key, value in parameters.items()) }
    </table>"""
    if not external:
        send_email(
            to=Variable.get('notification_emails'),
            subject=subject,
            html_content=html_msg,
            mime_charset='utf-8',
        )
    else:
        send_email(
            to=Variable.get('notification_emails_external'),
            subject=subject,
            html_content=html_msg,
            mime_charset='utf-8',
        )