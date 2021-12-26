#!/usr/bin/python
# -*- coding: latin-1 -*-
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
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

     
def get_order(id):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        
        itemMetadata = Fjson["itemMetadata"]
        ItemMetadata = itemMetadata["Items"]
        for x in ItemMetadata:
            itemMetadata_Id = x["Id"]
            itemMetadata_Seller = x["Seller"]
            itemMetadata_Name = x["Name"]
            itemMetadata_SkuName = x["SkuName"]
            itemMetadata_ProductId = x["ProductId"]
            itemMetadata_RefId = x["RefId"]
            itemMetadata_Ean = x["Ean"]
            itemMetadata_ImageUrl = x["ImageUrl"]
            itemMetadata_DetailUrl = x["DetailUrl"]
            df1 = pd.DataFrame({
                'orderId': id,
                'Id': itemMetadata_Id,
                'Seller': itemMetadata_Seller,
                'Name': itemMetadata_Name,
                'SkuName': itemMetadata_SkuName,
                'ProductId': itemMetadata_ProductId,
                'RefId': itemMetadata_RefId,
                'Ean': itemMetadata_Ean,
                'ImageUrl': itemMetadata_ImageUrl,
                'DetailUrl': itemMetadata_DetailUrl}, index=[0])
            init.df = init.df.append(df1)
        
        
    except:
        print("vacio")

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_vtex_item_metadata` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_vtex_item_metadata`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")


def run():
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    table_schema = [
        {
            "name": "ImageUrl",
            "type": "STRING",
            "mode": "NULLABLE"
    },{
        "name": "Ean",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "Seller",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "SkuName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "ProductId",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "Name",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "Id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DetailUrl",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "RefId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "orderId",
        "type": "STRING",
        "mode": "NULLABLE"
    }]
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_vtex_item_metadata'
    
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
        job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job = client.load_table_from_json(json_object, table, job_config = job_config)
        print(job.result())
        delete_duplicate()         


def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.test.shopstar_vtex_item_metadata`))')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId)
        print("Registro: "+str(registro))
    run()
        
    
get_params()