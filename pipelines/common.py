"""Common shared code used in pipelines."""
import os
import smtplib
from datetime import datetime
from smtplib import SMTPException
from decimal import Decimal
from hdbcli import dbapi
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException
from pandas import melt, read_csv, to_datetime, merge, DataFrame, json_normalize
from numpy import sum as npsum
from numpy import ndarray, nan
import itertools
import requests
from requests.auth import HTTPBasicAuth
import json
from re import match


import requests
import json

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
                    if column in ("CALDAY", "BUDAT", "BW_CALDAY_WEEK", "CALDAY_prevyear", "CALDAY_WEEK_prevyear") \
                    and value is not None:
                        if value == '': # Special sitiation cosidered in leap year's relative.
                            value = datetime.strptime('20200229', "%Y%m%d").date()
                        else:
                            value = datetime.strptime(value, "%Y%m%d").date()
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
                            value = datetime.strptime(value, "%Y%m%d").date()
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
            ar_row = ar_row.values.tolist() #magic word, transform DF to list
            list(itertools.chain.from_iterable(ar_row)) #magic word, rechaine nested list.

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




def execute_sfsf_to_ch(sfsf_auth, clickhouse_client, ch_table, last_mod_date=None, truncate=False):
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
    ch_truncate_sql = f"TRUNCATE TABLE {ch_table}"
    ch_insert_sql = f"INSERT INTO {ch_table} VALUES"
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
        print(f"Truncating ClickHouse table {ch_table}...")
        clickhouse_client.execute(ch_truncate_sql)
        table_truncated = True
    
    print("Start to send GET requests to SFSF...")
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
            print(f'The pipeline got {int(skips+1000)} rows from {entity}')
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
                print(f'Now, rows are just {len(df)} and that means it is the bottom of table.')
                break

        print(f'Finally, the pipeline requests {int(skips/1000)} times')

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
    print('Creating hierarchical master...')
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
    print('Genelating HR Master')
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
    print(f"Inserting {len(payload)} rows into ClickHouse {ch_table}.")
    clickhouse_client.execute(ch_insert_sql, payload)
    row_number += len(payload)
    print(f"\nTotal number of CCBJI group employees: {row_number}")
            
    
    return {"row_number": row_number, "truncated": table_truncated}


def get_clickhouse_client(environment="commercial", host="10.212.47.162", debug=False):
    """Connect to ClickHouse database and return Client object"""
    client = Client(
        host=host,
        #user=os.environ["CH_USER"],
        #password=os.environ["CH_PASS"],
        user='default',
        password='',
        database=environment
    )
    try: 
        client.connection.connect()
    except ServerException:
        raise ServerException("Connection to ClickHouse failed. Please check your connection information.")
    
    return client

# TODO: Wrap into a class
def get_hana_cursor(host='pjhw5100.c1ccej.local'): # PJH as workaround of PJI migration.
    """Connect to HANA database and return cursor"""   
    connection = dbapi.connect(
        address=host,
        port='30015',
        user=os.environ["HANA_USER"],
        password=os.environ["HANA_PASS"]
    )
    if not connection.isconnected():
        raise ConnectionError("Connection to HANA database unsuccessful. Please check your proxy and settings.")
    else:
        print("Connection successful")
        return connection.cursor()




def get_trax_api_auth():
    """Connect to TRAX database and get responce by API"""
    #credentials
    project = os.environ["TRAX_PROJECT"]
    apikey = os.environ["TRAX_API_KEY"]
    url = f"https://services.traxretail.com/api/v4/{project}/entity/store"
    response_auth = requests.get(url, headers={"Authorization":f"{apikey}"}) #GET request to TRAX
    if not response_auth.ok:
        raise ConnectionError("Connection to TRAX database unsuccessful. Please check your credentials and settings.")
    print('connection successful')
    return {"project" : project, "apikey" : apikey}

def get_sfsf_api_auth():
    """Connect to SFSF database and get responce by API"""
    #credentials
    userid = os.environ["SFSF_USER"]
    companyid = os.environ["SFSF_COMPANY"]
    passwordsfsf = os.environ["SFSF_PASS"]
    url = "https://api10.successfactors.com/odata/v2"
    response_auth = requests.get(url, auth=HTTPBasicAuth(userid+"@"+companyid, passwordsfsf)) 
    if not response_auth.ok:
        raise ConnectionError("Connection to SFSF database unsuccessful. Please check your credentials and settings.")
    print('connection successful')
    return {"userid" : userid, "companyid" : companyid, "password" : passwordsfsf}


    

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