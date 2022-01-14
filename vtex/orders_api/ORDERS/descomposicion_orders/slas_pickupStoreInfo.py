import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from os.path import join

class init:
  df = pd.DataFrame()

def get_order(id):
    try:
        headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=headers)
        Fjson = json.loads(response.text)
        
        shippingData = Fjson["shippingData"]
        shipping_logisticsInfo = shippingData["logisticsInfo"]
        
        for x in shipping_logisticsInfo:
            slas = x["slas"]
            for y in slas:
                pickupStoreInfo = y["pickupStoreInfo"]
                
                additionalInfo = pickupStoreInfo["additionalInfo"]
                address = pickupStoreInfo["address"]
                friendlyName = pickupStoreInfo["friendlyName"]
                isPickupStore = pickupStoreInfo["isPickupStore"]
                
                df1 = pd.DataFrame({
                    'orderId': id,
                    'additionalInfo': additionalInfo,
                    'address': address,
                    'isPickupStore': isPickupStore}, index=[0])
                init.df = init.df.append(df1)
    except:
        print(id)
        print("Vacio")

def delete_duplicate():
  try:
    print("Eliminando duplicados")
    client = bigquery.Client()
    QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.slas_pickupStoreInfo` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.slas_pickupStoreInfo`')
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
        table_id = 'slas_pickupStoreInfo'
        
        if df.empty:
            print('DataFrame is empty!')
        else:
            client  = bigquery.Client(project = project_id)
            dataset  = client.dataset(dataset_id)
            table = dataset.table(table_id)
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = "WRITE_TRUNCATE"
            job_config.autodetect = True
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job = client.load_table_from_json(json_object, table, job_config = job_config)
            print(job.result())
            delete_duplicate()
    except:
        print("Error.")
        logging.exception("message")

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.staging_zone.slas_pickupStoreInfo`))')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId)
        print("Registro: "+str(registro))
    run()

get_params()
