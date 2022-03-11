import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
from requests import request
import requests
from os.path import join
import logging
from datetime import date
import datetime
from datetime import timedelta
from os import system
from datetime import date, timedelta

class init:
  df = pd.DataFrame()
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
  dt = datetime.datetime.today()
  year = dt.year
  month = dt.month
  day = dt.day
  today = datetime.date.today()
  yesterday = today - datetime.timedelta(days=1)
  now = datetime.datetime.now()


def sku():
    try:
        print("Cargando consulta")
        client = bigquery.Client()
        QUERY = ('SELECT id_ean FROM `shopstar-datalake.staging_zone.shopstar_vtex_ean_id_temp`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        registro = 0
        for row in rows:
            url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyean/"+str(row.id_ean)+""
            response = requests.request("GET", url, headers=init.headers)
            
            if response.status_code == 200:
                if response.text is not '':
                    Fjson = json.loads(response.text)

                    IsProductActive    = Fjson["IsProductActive"]
                    ProductIsVisible    = Fjson["ProductIsVisible"]
                    ReleaseDate    = Fjson["ReleaseDate"]
                    UnitMultiplier = Fjson["UnitMultiplier"]
                    EstimatedDateArrival    = Fjson["EstimatedDateArrival"]
                    ShowIfNotAvailable    = Fjson["ShowIfNotAvailable"]
                    ModalType    = Fjson["ModalType"]
                    RewardValue    = Fjson["RewardValue"]
                    ProductGlobalCategoryId    = Fjson["ProductGlobalCategoryId"]
                    IsDirectCategoryActive    = Fjson["IsDirectCategoryActive"]
                    IsKit    = Fjson["IsKit"]
                    IsBrandActive    = Fjson["IsBrandActive"]
                    ProductId    = Fjson["ProductId"]
                    IsGiftCardRecharge    = Fjson["IsGiftCardRecharge"]
                    KeyWords    = Fjson["KeyWords"]
                    IsInventoried    = Fjson["IsInventoried"]
                    BrandName    = Fjson["BrandName"]
                    CSCIdentification    = Fjson["CSCIdentification"]
                    CommercialConditionId    = Fjson["CommercialConditionId"]
                    IsTransported    = Fjson["IsTransported"]
                    ProductCategoryIds    = Fjson["ProductCategoryIds"]
                    SkuName    = Fjson["SkuName"]
                    ProductRefId    = Fjson["ProductRefId"]
                    InformationSource    = Fjson["InformationSource"]
                    BrandId    = Fjson["BrandId"]
                    ProductFinalScore    = Fjson["ProductFinalScore"]
                    NameComplete    = Fjson["NameComplete"]
                    IsActive    = Fjson["IsActive"]
                    ProductDescription    = Fjson["ProductDescription"]
                    Id_ean    = Fjson["Id"]
                    TaxCode    = Fjson["TaxCode"]
                    ComplementName    = Fjson["ComplementName"]
                    MeasurementUnit    = Fjson["MeasurementUnit"]
                    ManufacturerCode    = Fjson["ManufacturerCode"]
                    DetailUrl    = Fjson["DetailUrl"]
                    ImageUrl    = Fjson["ImageUrl"]
                    ProductName    = Fjson["ProductName"]
                    
                    df1 = pd.DataFrame({
                        'IsProductActive': IsProductActive,
                        'ProductIsVisible': ProductIsVisible,
                        'ReleaseDate': ReleaseDate,
                        'UnitMultiplier': UnitMultiplier,
                        'EstimatedDateArrival': EstimatedDateArrival,
                        'ShowIfNotAvailable': ShowIfNotAvailable,
                        'ModalType': ModalType,
                        'RewardValue': RewardValue,
                        'ProductGlobalCategoryId': ProductGlobalCategoryId,
                        'IsDirectCategoryActive': IsDirectCategoryActive,
                        'IsKit': IsKit,
                        'IsBrandActive': IsBrandActive,
                        'ProductId': ProductId,
                        'IsGiftCardRecharge': IsGiftCardRecharge,
                        'KeyWords': KeyWords,
                        'IsInventoried': IsInventoried,
                        'BrandName': BrandName,
                        'CSCIdentification': CSCIdentification,
                        'CommercialConditionId': CommercialConditionId,
                        'IsTransported': IsTransported,
                        'ProductCategoryIds': ProductCategoryIds,
                        'SkuName': SkuName,
                        'ProductRefId': ProductRefId,
                        'InformationSource': InformationSource,
                        'BrandId': BrandId,
                        'ProductFinalScore': ProductFinalScore,
                        'NameComplete': NameComplete,
                        'IsActive': IsActive,
                        'ProductDescription': ProductDescription,
                        'Id_ean': Id_ean,
                        'TaxCode': TaxCode,
                        'ComplementName': ComplementName,
                        'MeasurementUnit': MeasurementUnit,
                        'ManufacturerCode': ManufacturerCode,
                        'DetailUrl': DetailUrl,
                        'ImageUrl': ImageUrl,
                        'ProductName': ProductName}, index=[0])
                    init.df = init.df.append(df1)
                    registro += 1
                    print("Registro: "+str(registro))
                    if registro == 100:
                        run()
                    if registro == 200:
                        run()
        run()
    except:
        print("Error.")
        logging.exception("message")

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def delete_duplicate():
    client = bigquery.Client()
    QUERY = (
        'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_ean_id_temp` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_ean_id_temp`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    print(rows)

def run():
    try:
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        table_schema = [
        {
            "name": "IsProductActive",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "ProductIsVisible",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "ReleaseDate",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },{
            "name": "UnitMultiplier",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "EstimatedDateArrival",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ShowIfNotAvailable",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "ModalType",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "RewardValue",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "ProductGlobalCategoryId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "IsDirectCategoryActive",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "IsKit",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "IsBrandActive",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "ProductId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "IsGiftCardRecharge",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "KeyWords",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "IsInventoried",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "BrandName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "CSCIdentification",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "CommercialConditionId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "IsTransported",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "ProductCategoryIds",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "SkuName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ProductRefId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "InformationSource",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "BrandId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "ProductFinalScore",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "NameComplete",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "IsActive",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "ProductDescription",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "Id_ean",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "TaxCode",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ComplementName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "MeasurementUnit",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ManufacturerCode",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "DetailUrl",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ImageUrl",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ProductName",
            "type": "STRING",
            "mode": "NULLABLE"
        }]
        
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_ean'
        
        
        
        if df.empty:
            print('DataFrame is empty!')
        else:
            try:
                client  = bigquery.Client(project = project_id)
                dataset  = client.dataset(dataset_id)
                table = dataset.table(table_id)
                job_config = bigquery.LoadJobConfig()
                job_config.schema = format_schema(table_schema)
                #job_config.write_disposition = "WRITE_TRUNCATE"
                #job_config.autodetect = True
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                job = client.load_table_from_json(json_object, table, job_config = job_config)
                print(job.result())
                delete_duplicate()
            except:
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
        
sku()