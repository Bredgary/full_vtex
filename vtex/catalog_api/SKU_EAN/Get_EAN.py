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
    ProductId = None
    NameComplete = None
    ComplementName = None
    ProductName = None
    ProductDescription = None
    ProductRefId = None
    TaxCode = None
    SkuName = None
    IsActive = None
    IsTransported = None
    IsInventoried = None
    IsGiftCardRecharge = None
    ImageUrl = None
    DetailUrl = None
    CSCIdentification = None
    BrandId = None
    BrandName = None
    IsBrandActive = None
    ManufacturerCode = None
    IsKit = None
    ProductCategoryIds = None
    IsDirectCategoryActive = None
    ProductGlobalCategoryId = None
    CommercialConditionId = None
    RewardValue = None
    EstimatedDateArrival = None
    MeasurementUnit = None
    UnitMultiplier = None
    InformationSource = None
    ModalType = None
    KeyWords = None
    ReleaseDate = None
    ProductIsVisible = None
    ShowIfNotAvailable = None
    IsProductActive = None
    ProductFinalScore = None
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
def decrypt_email(email):
    try:
        url = "https://conversationtracker.vtex.com.br/api/pvt/emailMapping?an=mercury&alias="+email+""
        headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
        response = requests.request("GET", url, headers=headers)
        formatoJ = json.loads(response.text)
        return formatoJ["email"]
    except:
        print("No se pudo desencriptar Email: "+str(email))
        
def get_order(id,reg):
    try:
        reg +=1
        url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyean/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        
        init.Id = Fjson["Id"]
        init.ProductId = Fjson["ProductId"]
        init.NameComplete = Fjson["NameComplete"]
        init.ComplementName = Fjson["ComplementName"]
        init.ProductName = Fjson["ProductName"]
        init.ProductDescription = Fjson["ProductDescription"]
        init.ProductRefId = Fjson["ProductRefId"]
        init.TaxCode = Fjson["TaxCode"]
        init.SkuName = Fjson["SkuName"]
        init.IsActive = Fjson["IsActive"]
        init.IsTransported = Fjson["IsTransported"]
        init.IsInventoried = Fjson["IsInventoried"]
        init.IsGiftCardRecharge = Fjson["IsGiftCardRecharge"]
        init.ImageUrl = Fjson["ImageUrl"]
        init.DetailUrl = Fjson["DetailUrl"]
        init.CSCIdentification = Fjson["CSCIdentification"]
        init.BrandId = Fjson["IdBrandId"]
        init.BrandName = Fjson["BrandName"]
        init.IsBrandActive = Fjson["IsBrandActive"]
        init.ManufacturerCode = Fjson["ManufacturerCode"]
        init.IsKit = Fjson["IsKit"]
        init.ProductCategoryIds = Fjson["ProductCategoryIds"]
        init.IsDirectCategoryActive = Fjson["IsDirectCategoryActive"]
        init.ProductGlobalCategoryId = Fjson["ProductGlobalCategoryId"]
        init.CommercialConditionId = Fjson["CommercialConditionId"]
        init.RewardValue = Fjson["RewardValue"]
        init.EstimatedDateArrival = Fjson["EstimatedDateArrival"]
        init.MeasurementUnit = Fjson["MeasurementUnit"]
        init.UnitMultiplier = Fjson["UnitMultiplier"]
        init.InformationSource = Fjson["InformationSource"]
        init.ModalType = Fjson["ModalType"]
        init.KeyWords = Fjson["KeyWords"]
        init.ReleaseDate = Fjson["ReleaseDate"]
        init.ProductIsVisible = Fjson["ProductIsVisible"]
        init.ShowIfNotAvailable = Fjson["ShowIfNotAvailable"]
        init.IsProductActive = Fjson["IsProductActive"]
        init.ProductFinalScore = Fjson["ProductFinalScore"]
        
        df1 = pd.DataFrame({
            'Id_ean': init.Id,
            'ProductId': init.ProductId,
            'NameComplete': init.NameComplete,
            'ComplementName': init.ComplementName,
            'ProductName': init.ProductName,
            'ProductDescription': init.ProductDescription,
            'ProductRefId': init.ProductRefId,
            'TaxCode': init.TaxCode,
            'SkuName': init.SkuName,
            'IsActive': init.IsActive,
            'IsTransported': init.IsTransported,
            'IsInventoried': init.IsInventoried,
            'IsGiftCardRecharge': init.IsGiftCardRecharge,
            'CSCIdentification': init.CSCIdentification,
            'BrandId': init.BrandId,
            'BrandName': init.BrandName,
            'IsBrandActive': init.IsBrandActive,
            'ManufacturerCode': init.ManufacturerCode,
            'IsKit': init.IsKit,
            'ProductCategoryIds': init.ProductCategoryIds,
            'IsDirectCategoryActive': init.IsDirectCategoryActive,
            'ProductGlobalCategoryId': init.ProductGlobalCategoryId,
            'CommercialConditionId': init.CommercialConditionId,
            'RewardValue': init.RewardValue,
            'EstimatedDateArrival': init.EstimatedDateArrival,
            'MeasurementUnit': init.MeasurementUnit,
            'UnitMultiplier': init.UnitMultiplier,
            'InformationSource': init.InformationSource,
            'ModalType': init.ModalType,
            'KeyWords': init.KeyWords,
            'ImageUrl': init.ImageUrl,
            'ReleaseDate': init.ReleaseDate,
            'ProductIsVisible': init.ProductIsVisible,
            'ShowIfNotAvailable': init.ShowIfNotAvailable,
            'IsProductActive': init.IsProductActive,
            'ProductFinalScore': init.ProductFinalScore,
            'DetailUrl': init.DetailUrl}, index=[0])
        init.df = init.df.append(df1)
        print("Registro: "+str(reg))
    except:
        print("vacio")
        
        
        
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
            'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_ean` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_ean`')
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
    table_id = 'shopstar_vtex_ean'
    
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