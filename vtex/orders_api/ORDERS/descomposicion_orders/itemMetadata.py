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
    '''
    itemMetadata
    '''
    itemMetadata_Id = None
    itemMetadata_Seller = None
    itemMetadata_Name = None
    itemMetadata_SkuName = None
    itemMetadata_ProductId = None
    itemMetadata_RefId = None
    itemMetadata_Ean = None
    itemMetadata_ImageUrl = None
    itemMetadata_DetailUrl = None
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

     
def get_order(id,reg):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
           
        try:
            itemMetadata = Fjson["itemMetadata"]
            ItemMetadata = itemMetadata["Items"]
        except:
            print("ItemMetadata. No tiene datos")
        try:
            for x in ItemMetadata:
                init.itemMetadata_Id = x["Id"]
                init.itemMetadata_Seller = x["Seller"]
                init.itemMetadata_Name = x["Name"]
                init.itemMetadata_SkuName = x["SkuName"]
                init.itemMetadata_ProductId = x["ProductId"]
                init.itemMetadata_RefId = x["RefId"]
                init.itemMetadata_Ean = x["Ean"]
                init.itemMetadata_ImageUrl = x["ImageUrl"]
                init.itemMetadata_DetailUrl = x["DetailUrl"]
        except:
            print("vacio")
        
    
        df1 = pd.DataFrame({
            'itemMetadata_Id': str(init.itemMetadata_Id),
            'itemMetadata_Seller': str(init.itemMetadata_Seller),
            'itemMetadata_Name': str(init.itemMetadata_Name),
            'itemMetadata_SkuName': str(init.itemMetadata_SkuName),
            'itemMetadata_ProductId': str(init.itemMetadata_ProductId),
            'itemMetadata_RefId': str(init.itemMetadata_RefId),
            'itemMetadata_Ean': str(init.itemMetadata_Ean),
            'itemMetadata_ImageUrl': str(init.itemMetadata_ImageUrl),
            'itemMetadata_DetailUrl': str(init.itemMetadata_DetailUrl)}, index=[0])
        init.df = init.df.append(df1)
        
        print("Registro: "+str(reg))
    except:
        print("vacio")

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_vtex_order` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_vtex_order`')
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
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_vtex_item_metadata'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()        


def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
    #QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.test.shopstar_vtex_order`))')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order("1014091072573-02",registro)
        run()
        if registro == 10000:
            run()
        if registro == 20000:
            run()
        if registro == 30000:
            run()
        if registro == 40000:
            run()
        if registro == 50000:
            run()
    run()
        
    
get_params()