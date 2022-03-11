import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
from requests import request
import requests
from os.path import join
import logging
from datetime import date
import datetime
from datetime import timedelta
from os import system
from datetime import date, timedelta

class init:
  df = pd.DataFrame()
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
  dt = datetime.datetime.today()
  year = dt.year
  month = dt.month
  day = dt.day
  today = datetime.date.today()
  yesterday = today - datetime.timedelta(days=1)
  now = datetime.datetime.now()


def sku():
    try:
        #print("Cargando consulta")
        #client = bigquery.Client()
        #QUERY = ('SELECT FieldValueId FROM `shopstar-datalake.staging_zone.shopstar_vtex_field_value`')
        #query_job = client.query(QUERY)
        #rows = query_job.result()
        registro = 0
        #for row in rows:
        url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/saleschannel/list"
        #url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/specificationvalue/"+str(row.FieldValueId)+""
        response = requests.request("GET", url, headers=init.headers)
        
        if response.status_code == 200:
            if response.text is not '':
                Fjson = json.loads(response.text)
                
                for x in Fjson:
                    Origin = x["Origin"]
                    CurrencyFormatInfo = x["CurrencyFormatInfo"]
                    StartsWithCurrencySymbol = CurrencyFormatInfo["StartsWithCurrencySymbol"]
                    CurrencyGroupSize = CurrencyFormatInfo["CurrencyGroupSize"]
                    CurrencyDecimalSeparator = CurrencyFormatInfo["CurrencyDecimalSeparator"]
                    Id = x["Id"]
                    ProductClusterId = x["ProductClusterId"]
                    CultureInfo = x["CultureInfo"]
                    CurrencySymbol = x["CurrencySymbol"]
                    if type (x["CurrencyDecimalDigits"]) == int:
                        CurrencyDecimalDigits = x["CurrencyDecimalDigits"]
                    else:
                        CurrencyDecimalDigits = 0
                    
                    CurrencyCode = x["CurrencyCode"]
                    Position = x["Position"]
                    TimeZone = x["TimeZone"]
                    ConditionRule = x["ConditionRule"]
                    CurrencyLocale = x["CurrencyLocale"]
                    CurrencyGroupSeparator = CurrencyFormatInfo["CurrencyGroupSeparator"]
                    IsActive = x["IsActive"]
                    CountryCode = x["CountryCode"]
                    Name = x["Name"]
                    
                    df1 = pd.DataFrame({
                        'Origin': Origin,
                        'CurrencyGroupSeparator': CurrencyGroupSeparator,
                        'StartsWithCurrencySymbol': StartsWithCurrencySymbol,
                        'CurrencyGroupSize': CurrencyGroupSize,
                        'CurrencyDecimalSeparator': CurrencyDecimalSeparator,
                        'Id': Id,
                        'ProductClusterId': ProductClusterId,
                        'CultureInfo': CultureInfo,
                        'CurrencySymbol': CurrencySymbol,
                        'CurrencyDecimalDigits': CurrencyDecimalDigits,
                        'CurrencyCode': CurrencyCode,
                        'Position': Position,
                        'TimeZone': TimeZone,
                        'ConditionRule': ConditionRule,
                        'CurrencyLocale': CurrencyLocale,
                        'IsActive': IsActive,
                        'CountryCode': CountryCode,
                        'Name': Name}, index=[0])
                    init.df = init.df.append(df1)
                    registro += 1
                    print("Registro: "+str(registro))
                    if registro == 100:
                        run()
                    if registro == 200:
                        run()
                    if registro == 300:
                        break
        run()
    except:
        print("Error.")
        logging.exception("message")

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def delete_duplicate():
    client = bigquery.Client()
    QUERY = (
        'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_field_value` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_field_value`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    print(rows)

def run():
    try:
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        table_schema = [
        {
            "name": "Origin",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "StartsWithCurrencySymbol",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "CurrencyGroupSize",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "CurrencyDecimalSeparator",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "Id",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "ProductClusterId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "CultureInfo",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "CurrencySymbol",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "CurrencyDecimalDigits",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "CurrencyCode",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "Position",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "TimeZone",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ConditionRule",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "CurrencyLocale",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "CurrencyGroupSeparator",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "IsActive",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "CountryCode",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "Name",
            "type": "STRING",
            "mode": "NULLABLE"
        }]  

        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_sales_channel_list'
        
        
        if df.empty:
            print('DataFrame is empty!')
        else:
            try:
                client  = bigquery.Client(project = project_id)
                dataset  = client.dataset(dataset_id)
                table = dataset.table(table_id)
                job_config = bigquery.LoadJobConfig()
                job_config.schema = format_schema(table_schema)
                #job_config.write_disposition = "WRITE_TRUNCATE"
                #job_config.autodetect = True
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                job = client.load_table_from_json(json_object, table, job_config = job_config)
                print(job.result())
                delete_duplicate()
            except:
                client  = bigquery.Client(project = project_id)
                dataset  = client.dataset(dataset_id)
                table = dataset.table(table_id)
                job_config = bigquery.LoadJobConfig()
                #job_config.write_disposition = "WRITE_TRUNCATE"
                #job_config.autodetect = True
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                job = client.load_table_from_json(json_object, table, job_config = job_config)
                print(job.result())
                delete_duplicate()
    except:
        print("Error.")
        logging.exception("message")
        
sku()