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
        print("Cargando consulta")
        client = bigquery.Client()
        QUERY = ('SELECT id_ean FROM `shopstar-datalake.staging_zone.shopstar_vtex_ean_id_temp`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        registro = 0
        for row in rows:
            url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyean/"+str(row.id_ean)+""
            response = requests.request("GET", url, headers=init.headers)
            
            if response.status_code == 200:
                if response.text is not '':
                    Fjson = json.loads(response.text)
                    
                    for x in Fjson:
                        SpecificationValue = x["SpecificationValue"]
                        SpecificationName = x["SpecificationName"]
                        SkuId = x["SkuId"]
                        Id = x["Id"]
                    
                        df1 = pd.DataFrame({
                            'SpecificationValue': SpecificationValue,
                            'SpecificationName': SpecificationName,
                            'SkuId': SkuId,
                            'Id': Id}, index=[0])
                        init.df = init.df.append(df1)
                        registro += 1
                        print("Registro: "+str(registro))
                        if registro == 100:
                            run()
                        if registro == 200:
                            run()
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
        'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_ean_id_temp` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_ean_id_temp`')
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
            "name": "SpecificationValue",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "SpecificationName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "SkuId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "Id",
            "type": "INTEGER",
            "mode": "NULLABLE"
        }]
        
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_sku_non_strtuctured'
        
        
        
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