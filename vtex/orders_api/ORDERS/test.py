import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join
import logging

class init:
    productList = []
    df = pd.DataFrame()
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def get_order(id,reg):
    try:
        df1 = pd.DataFrame
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        paymentData = Fjson["paymentData"]
        transactions = paymentData["transactions"]
        for x in transactions:
            payments = x["payments"]
            for x in payments:
                connectorResponses = x["connectorResponses"]
                Tid = connectorResponses["Tid"]
                ReturnCode= connectorResponses["ReturnCode"]
                Message= connectorResponses["Message"]
                try:
                    authId= connectorResponses["authId"]
                    orderId= connectorResponses["orderId"]
                except:
                    authId= None
                    orderId= None
                state= connectorResponses["state"]
                
                df1 = pd.DataFrame({
                    'orderId': id,
                    'Tid': Tid,
                    'ReturnCode': ReturnCode,
                    'Message': Message,
                    'authId': authId,
                    'C_orderId': orderId,
                    'state': state}, index=[0])
                init.df = init.df.append(df1)
                print("Registro: "+str(reg))
        if df1.empty:
            df1 = pd.DataFrame({
                'orderId': id}, index=[0])
            init.df = init.df.append(df1)
    except:
        df1 = pd.DataFrame({
            'orderId': id}, index=[0])
        init.df = init.df.append(df1)
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_order_connectorResponses` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_order_connectorResponses`')
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
        
        table_schema = [{
            "name": "state",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "C_orderId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "Tid",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "authId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "Message",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ReturnCode",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "orderId",
            "type": "STRING",
            "mode": "NULLABLE"
        }]    
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_order_connectorResponses'
        
        if df.empty:
            print('DataFrame is empty!')
        else:
            try:
                client  = bigquery.Client(project = project_id)
                dataset  = client.dataset(dataset_id)
                table = dataset.table(table_id)
                job_config = bigquery.LoadJobConfig()
                job_config.schema = format_schema(table_schema)
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                job = client.load_table_from_json(json_object, table, job_config = job_config)
                print(job.result())
                delete_duplicate()
            except:
                client  = bigquery.Client(project = project_id)
                dataset  = client.dataset(dataset_id)
                table = dataset.table(table_id)
                job_config = bigquery.LoadJobConfig()
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
    QUERY = ('SELECT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_order_connectorResponses`))')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId,registro)
        if registro == 10:
            run()
        if registro == 20:
            run()
        if registro == 30:
            run()
        if registro == 40:
            run()
        if registro == 50:
            run()
    run()
    
get_params()