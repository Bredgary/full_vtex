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
            shipping_logisticsInfo_itemIndex = x["itemIndex"]
            shipping_logisticsInfo_selectedSla = x["selectedSla"]
            shipping_logisticsInfo_lockTTL = x["lockTTL"]
            shipping_logisticsInfo_price = x["price"]
            shipping_logisticsInfo_listPrice = x["listPrice"]
            shipping_logisticsInfo_sellingPrice = x["sellingPrice"]
            shipping_logisticsInfo_deliveryWindow = x["deliveryWindow"]
            shipping_logisticsInfo_deliveryCompany = x["deliveryCompany"]
            shipping_logisticsInfo_shippingEstimate = x["shippingEstimate"]
            shipping_logisticsInfo_shippingEstimateDate = x["shippingEstimateDate"]
            shipping_logisticsInfo_deliveryChannel = x["deliveryChannel"]
            shipping_logisticsInfo_addressId = x["addressId"]
            shipping_logisticsInfo_polygonName = x["polygonName"]
            
            df1 = pd.DataFrame({
                'orderId': id,
                'itemIndex': shipping_logisticsInfo_itemIndex,
                'selectedSla': shipping_logisticsInfo_selectedSla,
                'lockTTL': shipping_logisticsInfo_lockTTL,
                'price': shipping_logisticsInfo_price,
                'listPrice': shipping_logisticsInfo_listPrice,
                'sellingPrice':shipping_logisticsInfo_sellingPrice,
                'deliveryWindow': shipping_logisticsInfo_deliveryWindow,
                'deliveryCompany': shipping_logisticsInfo_deliveryCompany,
                'shippingEstimate': shipping_logisticsInfo_shippingEstimate,
                'shippingEstimateDate': shipping_logisticsInfo_shippingEstimateDate,
                'deliveryChannel': shipping_logisticsInfo_deliveryChannel,
                'addressId': shipping_logisticsInfo_addressId,
                'polygonName': shipping_logisticsInfo_polygonName}, index=[0])
            init.df = init.df.append(df1)
    except:
        print("Vacio")

def delete_duplicate():
  try:
    print("Eliminando duplicados")
    client = bigquery.Client()
    QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shipping_logisticsInfo` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shipping_logisticsInfo`')
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
        table_id = 'shipping_logisticsInfo'
        
        if df.empty:
            print('DataFrame is empty!')
        else:
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

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.staging_zone.shipping_logisticsInfo`))')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId)
        print("Registro: "+str(registro))
    run()

get_params()
