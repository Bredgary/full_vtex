import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone

class init:
    productList = []
    df = pd.DataFrame()
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def get_sk_context(id,reg):
    #try:
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyid/"+str(id)+""
    querystring = {"sc":"1"}
    response = requests.request("GET", url, headers=init.headers)
    Fjson = json.loads(response.text)
    #del Fjson["Dimension"]
    del Fjson["RealDimension"]
    del Fjson["ManufacturerCode"]
    del Fjson["KitItems"]
    del Fjson["Services"]
    del Fjson["Categories"]
    del Fjson["CategoriesFullPath"]
    del Fjson["Attachments"]
    del Fjson["Collections"]
    del Fjson["SkuSellers"]
    del Fjson["SalesChannels"]
    del Fjson["Images"]
    del Fjson["Videos"]
    del Fjson["SkuSpecifications"]
    del Fjson["ProductSpecifications"]
    del Fjson["ProductClustersIds"]
    del Fjson["PositionsInClusters"]
    del Fjson["ProductClusterNames"]
    del Fjson["ProductClusterHighlights"]
    del Fjson["ProductCategoryIds"]
    del Fjson["IsDirectCategoryActive"]
    del Fjson["ProductGlobalCategoryId"]
    del Fjson["ProductCategories"]
    del Fjson["CommercialConditionId"]
    del Fjson["RewardValue"]
    del Fjson["AlternateIds"]
    del Fjson["AlternateIdValues"]
    del Fjson["EstimatedDateArrival"]
    del Fjson["MeasurementUnit"]
    del Fjson["UnitMultiplier"]
    del Fjson["InformationSource"]
    del Fjson["ModalType"]
    del Fjson["KeyWords"]
    del Fjson["ReleaseDate"]
    del Fjson["ProductIsVisible"]
    del Fjson["ShowIfNotAvailable"]
    del Fjson["IsProductActive"]
    del Fjson["ProductFinalScore"]
    
    df1 = pd.DataFrame({
        'Id': Fjson["Id"],
        'ProductId': Fjson["ProductId"]}, index=[0])
    init.df = init.df.append(df1)
    print("Registro: "+str(reg))
    #except:
    #   print("Vacio")


def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_SKU_ID`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        get_sk_context(944,registro)
        registro += 1
        if registro == 1:
            break



def delete_duplicate():
    try:
        print("Eliminando Duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.landing_zone.shopstar_vtex_sku_context` AS SELECT DISTINCT * FROM `shopstar-datalake.landing_zone.shopstar_vtex_sku_context`')
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
    print(json_object)
    
    project_id = '999847639598'
    dataset_id = 'landing_zone'
    table_id = 'shopstar_vtex_sku_context_'

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
