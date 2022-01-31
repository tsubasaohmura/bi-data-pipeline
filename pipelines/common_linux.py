"""Common shared code used in pipelines."""
import os
import smtplib
from datetime import datetime
from smtplib import SMTPException
from decimal import Decimal

from hdbcli import dbapi
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException
from numpy.lib.shape_base import apply_along_axis
#for AR data
from pandas import melt, read_csv, to_datetime, merge, DataFrame, json_normalize
from numpy import sum as npsum
from numpy import ndarray
import itertools

import requests
import json

# Temporarily enable proxy from Linux server to the Internet. 
os.environ['http_proxy']="http://10.139.60.231:8080"
os.environ['https_proxy']="https://10.139.60.231:8080"

LOCAL_TIMEZONE='Asia/Tokyo'

def clean_up(hana=None, clickhouse=None):
    if hana:
        hana.connection.close()
    if clickhouse:
        clickhouse.disconnect()

def execute_hana_to_ch(hana_cursor, clickhouse_client, sql_hana, ch_table, max_fetch_rows, \
    daily_sales_transform=False, equipment_transform=False, equipment_cust=False, truncate=False):
    """Execute sql on HANA database and insert into ClickHouse in max_fetch_rows batches.
    Args:
        hana (pyhdbcli.Cursor): instance of HANA database cursor
        clickhouse (clickhouse_driver.client.Client): instance of ClickHouse client
        sql_hana (str): SELECT SQL query to HANA
        ch_table (str): destination table in ClickHouse
        max_fetch_rows (int): how many rows to fetch from HANA per one batch.
        daily_sales_transform (bool): whether to do data transforms specific to sales_detail HANA table
        equipment_transform (bool): whether to do data transforms specific to Placement HANA table 
        truncate (bool): whether to truncate ClickHouse table before inserting
    Returns:
        dict:
            row_number (int): total number of ingested rows
            truncated (bool): whether ch_table was truncated or not
    """
    
    ch_truncate_sql = f"TRUNCATE TABLE {ch_table}"
    ch_insert_sql = f"INSERT INTO {ch_table} VALUES"

    print("Sending query to HANA...")
    hana_response = hana_cursor.execute(sql_hana)
    if hana_response is False:
        raise Exception("Cursor failed to execute SQL query.")

    table_truncated = False
    row_number = 0

    while True:
        print(f"Fetching {row_number}~ rows...")
        result = hana_cursor.fetchmany(max_fetch_rows)
        if len(result) == 0 and row_number == 0: raise Exception("Failed to fetch any data.")
        
        if truncate and not table_truncated:
            print(f"Truncating ClickHouse table {ch_table}...")
            clickhouse_client.execute(ch_truncate_sql)
            table_truncated = True
        
        if daily_sales_transform:
            payload = []
            for record in result:
                transformed_record = []
                for column, value in zip(record.column_names, record.column_values):
                    if column in ("CALMONTH") and value is not None: 
                        value = value[:4] + '-' + value[4:]
                    if column in ("CALDAY", "BUDAT", "BW_CALDAY_WEEK", "CALDAY_prevyear", "CALDAY_WEEK_prevyear", "0CALDAY_WEEK_prevyear") \
                    and value is not None:
                        if value == '':
                            value = '21990101'
                        else:
                            value = datetime.strptime(value, "%Y%m%d").date()
                    if value is None:
                        value = ''
                    if isinstance(value, Decimal):
                        value = str(value)
                    transformed_record.append(value)
                assert len(transformed_record) == len(record)
                payload.append(tuple(transformed_record))
            print('Sales Detail table has been transformed.')
        elif equipment_transform:
            print("Now the script shift to equipment_transform section")
            payload = []
            for record in result:
                transformed_record = []
                for column, value in zip(record.column_names, record.column_values):
                    if column in ("ADDAT", "IDAT2") and value is not None:
                        if value == '00000000':
                            value = '20990101'
                        else:
                            value = datetime.strptime(value, "%Y%m%d").date()
                    if isinstance(value, Decimal):
                        value = str(value)
                    if value is None:
                        value = ""
                    transformed_record.append(value)
                assert len(transformed_record) == len(record)
                payload.append(tuple(transformed_record))            
        elif equipment_cust:
            print("Now the script shift to equipment_cust section")
            payload = []
            for record in result:
                transformed_record = []
                for column, value in zip(record.column_names,  record.column_values): 
                    if isinstance(value, Decimal):
                        value = str(value)
                    if value is None:
                        value = ""
                    transformed_record.append(value)
                assert len(transformed_record) == len(record)
                payload.append(tuple(transformed_record))
        else:
            payload = result

        print(f"Inserting {len(payload)} rows into ClickHouse {ch_table}.")
        clickhouse_client.execute(ch_insert_sql, payload)
        row_number += len(payload)
        if len(payload) < max_fetch_rows:
            print("Reached the end of query result set.")
            print(f"Total number of rows: {row_number}")
            break
    
    return {"row_number": row_number, "truncated": table_truncated}

def execute_trax_to_ch(clickhouse_client, ch_table, max_fetch_rows, credentials,\
    master=False, truncate=False):
    """Extract TRAX Retail data via TRAX API. This script supports both bulk-Analysis transactiondata and master.
    Args:
        clickhouse (clickhouse_driver.client.Client): instance of ClickHouse client
        ch_table (str): destination table in ClickHouse
        max_fetch_rows (int): how many rows to fetch from HANA per one batch.
        credentials(dict): Contain credentials to request TRAX API.
        master (str): define the master table type.(e.x. store master)
        truncate (bool): whether to truncate ClickHouse table before inserting
    Returns:
        dict:
            row_number (int): total number of ingested rows
            truncated (bool): whether ch_table was truncated or not
    """
    
    ch_truncate_sql = f"TRUNCATE TABLE {ch_table}"    
    ch_insert_sql = f"INSERT INTO {ch_table} VALUES"

    table_truncated = False
    row_number = 0

    trax_project = credentials['project']
    trax_api_key = credentials['apikey']
    
    print(f"Fetching {row_number}~ rows...")

    while True:    
        if truncate and not table_truncated:
            print(f"Truncating ClickHouse table {ch_table}...")
            clickhouse_client.execute(ch_truncate_sql)
            table_truncated = True
        
        if master:
            # Define specific arguments
            if master == 'store':
                entity_name = 'store?sort=store_number'

            # TRAX CUSTOMER MASTER
            url_trax = f"https://services.traxretail.com/api/v4/ccjp/entity/{entity_name}&page=1&per_page=500"
            link_list = [] #list for put session links
            master_customer_list = [{'Customer code' : "0M00000000", 'Store name' : "オークワ万代", 'Office code' : "dummy code", 'Sales center' : "dummySC",
            'Account code' : "00000000", 'Account name' : "オークワ", 'Channel' : "dummy", 'Segment' : "dummy", 'City' : "Kashiba", 'Prefecture' : "Nara"}]
            cols_master = ['Customer code', 'Store name', 'Office code', 'Sales center', 'Account code', 'Account name', 
            'Channel', 'Segment', 'City', 'Prefecture']
            table_master = DataFrame(index = [], columns = cols_master)

            sprint_link = 1
            while True: #loop to get every session links at specific date duration
                print(f'sprint{sprint_link}')
                response_trax_list = requests.get(url_trax, headers={"Authorization":f"{trax_api_key}"}) #GET request to TRAX
                result_trax_list = json.loads(response_trax_list.text)
                for link in range(len(result_trax_list["store"])): #loop to get 200 session links into "link_list"
                    place = result_trax_list["store"][link]
                    if 'additional_attribute_10' in place["store_additional_attributes"]:
                        channel = place["store_additional_attributes"]["additional_attribute_10"]
                    else:
                        channel = " "
                    link_list.append({'Customer code' : place["store_number"], 'Store name' : place["store_display_name"],
                                                    'Office code' : place["branch_name"], 'Sales center' : place["region_name"],
                                                    'Account code' : place["branch_name"], 
                                                    'Account name' : place["store_type_name"], 'Channel' : channel, 
                                                    'Segment' : "blank", 'City' : place["street"], 'Prefecture' : place["district_name"]})
                url_trax = "https://services.traxretail.com" + result_trax_list["metadata"]["links"]["next"] # over write the "url_trax_list"
                if result_trax_list["metadata"]["links"]["next"] == "": # when the page was the last page
                    break
                sprint_link += 1
            df_master = json_normalize(link_list)# Change to DataFrame
            table_master = table_master.append(df_master).astype(str)
            payload = table_master.values.tolist() # Transform DF to list
        else:
            print("Please check the prametor")
            break
        print(f"Inserting {len(payload)} rows into ClickHouse {ch_table}.")
        clickhouse_client.execute(ch_insert_sql, payload)
        row_number = len(payload)
        if row_number < max_fetch_rows:
            print("Reached the end of query result set.")
            print(f"Total number of trax_rows: {row_number}")
            break
        else:
            print("You have to define the max fetch rows again")
    
    return {"row_number": row_number, "truncated": table_truncated}


def execute_ar_to_ch(clickhouse_client, ch_table, ch_table_2, max_fetch_rows, \
    ar_transform=False, truncate=False, ar_data="ar_sample"):
    """transform AUGMENT ar csv data and insert into ClickHouse in max_fetch_rows batches.
    Args:
        clickhouse (clickhouse_driver.client.Client): instance of ClickHouse client
        ch_table (str): destination table in ClickHouse
        max_fetch_rows (int): how many rows to fetch from HANA per one batch.
        ar_transform (bool): whether to do data transforms specific to AUGMENT ar data table
        truncate (bool): whether to truncate ClickHouse table before inserting
    Returns:
        dict:
            row_number (int): total number of ingested rows
            truncated (bool): whether ch_table was truncated or not
    """
    
    ch_truncate_sql = f"TRUNCATE TABLE {ch_table}"
    ch_truncate_sql_2 = f"TRUNCATE TABLE {ch_table_2}"    
    ch_insert_sql = f"INSERT INTO {ch_table} VALUES"
    ch_insert_sql_2 = f"INSERT INTO {ch_table_2} VALUES"

    table_truncated = False
    row_number = 0
    row_number_2 = 0
    
    print(f"Fetching {row_number}~ rows...")

    while True:    
        if truncate and not table_truncated:
            print(f"Truncating ClickHouse table {ch_table}...")
            clickhouse_client.execute(ch_truncate_sql)
            clickhouse_client.execute(ch_truncate_sql_2)
            table_truncated = True
        
        if ar_transform:
            ar = read_csv(f"{ar_data}.csv") #import csv file from laptop

            #unpivot the sheet and create transactional records table
            ar_datelist = list(ar.columns[1:])
            unpivot = melt(ar, id_vars=["Time"], value_vars=ar_datelist, var_name="Day")
            unpivot = unpivot.rename(columns={'Time': 'Email', 'value': 'Usage'}) #output
            unpivot.Email = unpivot.Email.astype('string')
            unpivot.loc[unpivot['Day'] == 'Amount', 'Day'] = "2099-01-01"
            unpivot.Day = to_datetime(unpivot.Day)
            unpivot = unpivot.values.tolist()

            #add sum() of rows to original ar data sheet
            ar_row = ar
            droplist = ar_row.drop('Time', axis=1)#create droplist for culculation
            number = npsum(droplist, axis=1)
            ar_row['Amount'] = number #add sum value
            ar_row = ar_row.rename(columns={'Time':'Email'})#just rename the column
            ar_row = ar_row[['Email','Amount']] #output : keep Email, Amount and drop other columns 
            ar_row.Email = ar_row.Email.astype('string') 
            ar_row = ar_row.values.tolist()
            list(itertools.chain.from_iterable(ar_row))

        else:
            print("Please check the prametor")
            break
        print(f"Inserting {len(ar_row)} rows into ClickHouse {ch_table}.")
        clickhouse_client.execute(ch_insert_sql, ar_row)
        print(f"Inserting {len(unpivot)} rows into ClickHouse {ch_table_2}.")
        clickhouse_client.execute(ch_insert_sql_2, unpivot)
        row_number = len(ar_row)
        row_number_2 += len(unpivot) 
        if row_number_2 < max_fetch_rows:
            print("Reached the end of query result set.")
            print(f"Total number of ar_rows: {row_number}")
            print(f"Total number of ar_transaction: {row_number_2}")
            break
        else:
            print("You have to define the max fetch rows again")
    
    return {"row_number": row_number, "truncated": table_truncated}



def get_clickhouse_client(environment="commercial"):
    """Connect to ClickHouse database and return Client object"""
    client = Client(
        host="10.212.47.162",
        user='tsubasa_com',
        password='tsubasa_com',
        database=environment
    )
    try: 
        client.connection.connect()
    except ServerException:
        raise ServerException("Connection to ClickHouse failed. Please check your connection information.")
    
    return client

# TODO: Wrap into a class
def get_hana_cursor(host='10.212.51.15'): # PJI
    """Connect to HANA database and return cursor"""   
    connection = dbapi.connect(
        address=host,
        port='30015',
        user=os.environ['HANA_USER'],
        password=os.environ['HANA_PASS']
    )
    if not connection.isconnected():
        raise ConnectionError("Connection to HANA database unsuccessful. Please check your proxy and settings.")
    else:
        print("Connection successful")
        return connection.cursor()



    

def send_notification(*, subject, message, receivers=['kirill.denisenko@ccbji.co.jp', 'abf90568.ccbji.onmicrosoft.com@apac.teams.ms'], \
    host='10.139.60.231', sender="bi-data-pipelines@ccbji.co.jp"):
    """Send job notification to specified emails."""
    msg = f"Subject: {subject}\n\n{message}"
    try:
        with smtplib.SMTP(host) as smtp:
            smtp.sendmail(sender, receivers, msg)
    except SMTPException as e:
        print("Failed to send an email notification:\n", e)


def send_success(job, rows, truncated, start_time, end_time, date=""):
    subject = f"[SUCCESS] {date} {job}"
    msg = f"""Pipeline moved data successfully.
Job name:\t {job}
Date:\t {date if date else "N/A"}
Total amount of rows:\t {rows}
Destination table truncated?:\t {truncated}
Start time:\t {str(start_time)}
End time:\t {str(end_time)}
Elapsed time:\t {str(end_time - start_time)}
"""
    send_notification(subject=subject, message=msg)


def send_failure(job, start_time, e_time, e):
    subject = f"[FAILURE] {job}"
    msg = f"""Job failed with exception!
Job name:\t {job}
Start time:\t {str(start_time)}
Exception time:\t {str(e_time)}
Exception:\t {str(e)}
"""
    send_notification(subject=subject, message=msg)