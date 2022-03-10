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
  listaSku = []
  now = datetime.datetime.now()


def sku():
    try:
        print("Cargando consulta")
        client = bigquery.Client()
        QUERY = ('SELECT SPLIT(`categoriesIds`, "/")[safe_ordinal(2)] AS `cat1`,SPLIT(`categoriesIds`, "/")[safe_ordinal(3)] AS `cat2`,SPLIT(`categoriesIds`, "/")[safe_ordinal(4)] AS `cat3` FROM `shopstar-datalake.staging_zone.shopstar_order_items` WHERE lastChange BETWEEN "'+str(init.year)+'-'+str(init.month)+'-'+str(init.day)+' 00:00:00" AND "'+str(init.year)+'-'+str(init.month)+'-'+str(init.day)+' 23:59:59"')
        query_job = client.query(QUERY)
        rows = query_job.result()
        registro = 0
        for row in rows:
            url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pub/specification/field/listTreeByCategoryId/"+str(row.cat1)+""

            response = requests.request("GET", url, headers=init.headers)
            
            if response.status_code == 200:
                if response.text is not '':
                    
                    Fjson = json.loads(response.text)
                    for x in Fjson:
                        
                        Name = x["Name"]
                        CategoryId = x["CategoryId"]
                        FieldId = x["FieldId"]
                        IsActive = x["IsActive"]
                        IsStockKeepingUnit = x["IsStockKeepingUnit"]
                        
                        df1 = pd.DataFrame({
                            'Name': Name,
                            'CategoryId': CategoryId,
                            'FieldId': FieldId,
                            'IsActive': IsActive,
                            'IsStockKeepingUnit': IsStockKeepingUnit}, index=[0])
                        init.df = init.df.append(df1)
                        registro += 1
                        print("Registro: "+str(registro))
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
        'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_sku_specification` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_sku_specification`')
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
            "name": "FieldId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "IsActive",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "CategoryId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "IsStockKeepingUnit",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "Name",
            "type": "STRING",
            "mode": "NULLABLE"
        }]
        

        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_sku_specification'
        
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
                #job_config.schema = format_schema(table_schema)
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