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
    Id = None
    Name = None
    IsActive = None
    ProductClusterId = None
    CountryCode = None
    CultureInfo = None
    TimeZone = None
    CurrencyCode = None
    CurrencySymbol = None
    CurrencyLocale = None
    CurrencyFormatInfo = None
    CurrencyDecimalDigits = None
    CurrencyDecimalSeparator = None
    CurrencyGroupSeparator = None
    CurrencyGroupSize = None
    StartsWithCurrencySymbol = None
    Origin = None
    Position = None
    ConditionRule = None
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
        
def get_order(reg):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/saleschannel/list"
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        init.Id = Fjson["Id"]
        init.Name = Fjson["Name"]
        init.IsActive = Fjson["IsActive"]
        init.ProductClusterId = Fjson["ProductClusterId"]
        init.CountryCode = Fjson["CountryCode"]
        init.CultureInfo = Fjson["CultureInfo"]
        init.TimeZone = Fjson["TimeZone"]
        init.CurrencyCode = Fjson["CurrencyCode"]
        init.CurrencySymbol = Fjson["CurrencySymbol"]
        init.CurrencyLocale = Fjson["CurrencyLocale"]
        CurrencyFormatInfo = Fjson["CurrencyFormatInfo"]
        init.CurrencyDecimalDigits = CurrencyFormatInfo["CurrencyDecimalDigits"]
        init.CurrencyDecimalSeparator = CurrencyFormatInfo["CurrencyDecimalSeparator"]
        init.CurrencyGroupSeparator = CurrencyFormatInfo["CurrencyGroupSeparator"]
        init.CurrencyGroupSize = CurrencyFormatInfo["CurrencyGroupSize"]
        init.StartsWithCurrencySymbol = CurrencyFormatInfo["StartsWithCurrencySymbol"]
        init.Origin = Fjson["Origin"]
        init.Position = Fjson["Position"]
        init.ConditionRule = Fjson["ConditionRule"]
        
        df1 = pd.DataFrame({
            'Id_ean': init.Id,
            'ProductId': init.Name,
            'NameComplete': init.IsActive,
            'ComplementName': init.ProductClusterId,
            'ProductName': init.CountryCode,
            'ProductDescription': init.CultureInfo,
            'ProductRefId': init.TimeZone,
            'TaxCode': init.CurrencyCode,
            'SkuName': init.CurrencySymbol,
            'IsActive': init.CurrencyLocale,
            'ProductFinalScore': init.CurrencyDecimalDigits,
            'ProductFinalScore': init.CurrencyDecimalSeparator,
            'ProductFinalScore': init.CurrencyGroupSeparator,
            'ProductFinalScore': init.CurrencyGroupSize,
            'ProductFinalScore': init.StartsWithCurrencySymbol,
            'ProductFinalScore': init.Origin,
            'ProductFinalScore': init.Position,
            'ProductFinalScore': init.ConditionRule,
            'DetailUrl': init.DetailUrl}, index=[0])
        init.df = init.df.append(df1)
        print("Registro: "+str(reg))
    except:
        print("Registro: "+str(reg))
        
        
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT id FROM `shopstar-datalake.staging_zone.shopstar_vtex_sku_ean_id`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.id,registro)
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_sales_channel_list` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_sales_channel_list`')
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
    table_id = 'shopstar_vtex_sales_channel_list'
    
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