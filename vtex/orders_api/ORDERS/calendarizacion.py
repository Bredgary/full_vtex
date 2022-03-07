import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from os.path import join
import logging
from datetime import date
import datetime

class init:
  df = pd.DataFrame()
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
  dt = datetime.datetime.today()
  year = dt.year
  month = dt.month
  day = dt.day

def getListSpecificationsGroupByCategory():
    try:
        print("Cargando consulta")
        client = bigquery.Client()
        QUERY = ('SELECT distinct productId, lastChange FROM `shopstar-datalake.staging_zone.shopstar_order_items` WHERE lastChange BETWEEN "'+str(init.year)+'-'+str(init.month)+'-'+str(init.day)+' 00:00:00" AND "'+str(init.year)+'-'+str(init.month)+'-'+str(init.day)+' 06:00:00"')
        query_job = client.query(QUERY)
        rows = query_job.result()
        registro = 0
        for row in rows:
            try:
                registro += 1
                url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/product/"+str(row.productId)+"/salespolicy"
                response = requests.request("GET", url, headers=init.headers)
                Fjson = json.loads(response.text)
                for x in Fjson:
                    df1 = pd.DataFrame({
                        'productId': row.productId,
                        'storeId': x["StoreId"]}, index=[0])
                    print("Registro: "+str(registro))
                    init.df = init.df.append(df1)
            except:
                df1 = pd.DataFrame({
                    'position': None,
                    'categoryId': row.productId}, index=[0])
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
        'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_product_trade_policy` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_product_trade_policy`')
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
                "name": "ProductId",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },{
                "name": "StoreId",
                "type": "INTEGER",
                "mode": "NULLABLE"
            }]

        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_product_trade_policy'

        client  = bigquery.Client(project = project_id)
        dataset  = client.dataset(dataset_id)
        table = dataset.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = format_schema(table_schema)
        job = client.load_table_from_json(json_object, table, job_config = job_config)
        print(job.result())
        delete_duplicate()
    except:
        print("Error.")
        logging.exception("message")

getListSpecificationsGroupByCategory()
