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
    
    Tid = None
    ReturnCode= None
    Message= None
    authId= None
    orderId= None
    state= None
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
def get_order(id,reg):
    try:
        reg +=1
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        try:
            paymentData = Fjson["paymentData"]
            transactions = paymentData["transactions"]
            for x in transactions:
                payments = x["payments"]
                for x in payments:
                    connectorResponses = x["connectorResponses"]
                    init.Tid = connectorResponses["Tid"]
                    init.ReturnCode= connectorResponses["ReturnCode"]
                    init.Message= connectorResponses["Message"]
                    init.authId= connectorResponses["authId"]
                    init.orderId= connectorResponses["orderId"]
                    init.state= connectorResponses["state"]
            print("Registro: "+str(reg))
        except:
            print("Registro: "+str(reg))
        df1 = pd.DataFrame({
            'orderId': str(id),
            'Tid': str(init.Tid),
            'ReturnCode': str(init.ReturnCode),
            'Message': str(init.Message),
            'authId': str(init.authId),
            'C_orderId': str(init.orderId),
            'state': str(init.state)}, index=[0])
        init.df = init.df.append(df1)
    except:
        print("Vacio")
        print("Registro: "+str(reg))
        
        
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
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
            'CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_order_connectorResponses` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_order_connectorResponses`')
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
    dataset_id = 'test'
    table_id = 'shopstar_order_connectorResponses'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()
    
run()