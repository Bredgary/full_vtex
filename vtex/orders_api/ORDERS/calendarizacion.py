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
def get_product_cal():
    try:
        print("Cargando consulta")
        client = bigquery.Client()
        QUERY = ('''SELECT 
        distinct productId, 
        lastChange
        FROM `shopstar-datalake.staging_zone.shopstar_order_items` 
        WHERE 
        lastChange BETWEEN "2022-03-02 01:00:00" AND "2022-03-02 02:00:00"''')
        query_job = client.query(QUERY)
        rows = query_job.result()
        registro = 0
        print(init.day)
        print(init.month)
        print(init.year)
        for row in rows:
            print(row.productId)
            #url = "https://mercury.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/"+str(row.id)+""
            #response = requests.request("GET", url, headers=init.headers)
            #Fjson = json.loads(response.text)
            #balance = Fjson["balance"]
            #registro_sku = 0
            #registro_sku += 1
            #print("registro_sku: "+str(registro_sku))
            #for x in balance:
                #warehouseId = x["warehouseId"]
                #warehouseName = x["warehouseName"]
                #totalQuantity = x["totalQuantity"]
                #reservedQuantity = x["reservedQuantity"]
                #hasUnlimitedQuantity = x["hasUnlimitedQuantity"]
                #timeToRefill = x["timeToRefill"]
                #dateOfSupplyUtc = x["dateOfSupplyUtc"]
                
                #df1 = pd.DataFrame({
                #    'SKU_ID': row.id,
                #    'warehouseId': warehouseId,
                #    'warehouseName': warehouseName,
                #    'totalQuantity': totalQuantity,
                #    'reservedQuantity': reservedQuantity,
                #    'hasUnlimitedQuantity': hasUnlimitedQuantity,
                #    'timeToRefill': timeToRefill,
                #    'dateOfSupplyUtc': dateOfSupplyUtc}, index=[0])
                #init.df = init.df.append(df1)
                #registro += 1
                #print("Registro: "+str(registro))
                #if registro == 10:
                #    run()
                #if registro == 20:
                #    run()
                #if registro == 30:
                #    run()
                #if registro == 40:
                #    run()
                #if registro == 50:
                #    run()
        #run()
    except:
        print("Vacio")


def delete_duplicate():
  try:
    print("Eliminando duplicados")
    client = bigquery.Client()
    QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_list_inventory_by_sku` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_inventory_by_sku`')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print(rows)
  except:
    print("Consulta SQL no ejecutada")


def run():
    try:
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_list_inventory_by_sku2'
        
        if df.empty:
            print('DataFrame is empty!')
        else:
            client  = bigquery.Client(project = project_id)
            dataset  = client.dataset(dataset_id)
            table = dataset.table(table_id)
            job_config = bigquery.LoadJobConfig()
            job_config.autodetect = True
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job = client.load_table_from_json(json_object, table, job_config = job_config)
            print(job.result())
            delete_duplicate()
    except:
        print("Error.")
        logging.exception("message")

get_product_cal()