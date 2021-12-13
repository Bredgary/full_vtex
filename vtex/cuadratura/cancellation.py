import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join

class init:
    productList = []
    df = pd.DataFrame()
    RequestedByUser = None
    RequestedBySystem = None
    RequestedBySellerNotification = None
    RequestedByPaymentNotification = None
    Reason = None
    CancellationDate = None
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
def get_order(id,reg):
    try:
        reg +=1
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        try:
            init.cancellationData = Fjson["cancellationData"]
            init.RequestedByUser = cancellationData["RequestedByUser"]
            init.RequestedBySystem = cancellationData["RequestedBySystem"]
            init.RequestedBySellerNotification = cancellationData["RequestedBySellerNotification"]
            init.RequestedByPaymentNotification = cancellationData["RequestedByPaymentNotification"]
            init.Reason = cancellationData["Reason"]
            init.CancellationDate = cancellationData["CancellationDate"]
            print("Registro: "+str(reg))
        except:
            print("Registro: "+str(reg))
        df1 = pd.DataFrame({
            'orderId': str(id),
            'RequestedByUser': str(init.RequestedByUser),
            'RequestedBySystem': str(init.RequestedBySystem),
            'RequestedBySellerNotification': str(init.RequestedBySellerNotification),
            'RequestedByPaymentNotification': str(init.RequestedByPaymentNotification),
            'Reason': str(init.Reason),
            'CancellationDate': str(init.CancellationDate)}, index=[0])
        init.df = init.df.append(df1)
    except:
        print("Vacio")
        print("Registro: "+str(reg))
        
        
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_order_cancellation`))')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 1
    for row in rows:
        get_order(row.orderId,registro)
        registro += 1
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_order_cancellation` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_order_cancellation`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")


def run():
    get_params()
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    project_id = '999847639598'
    dataset_id = 'staging_zone'
    table_id = 'shopstar_order_cancellation'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()
    
run()