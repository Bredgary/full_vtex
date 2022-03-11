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
  listaSku = []
  now = datetime.datetime.now()


def sku():
    try:
        print("Cargando consulta")
        client = bigquery.Client()
        QUERY = ('SELECT id FROM `shopstar-datalake.staging_zone.shopstar_vtex_sku_id_temp`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        registro = 0
        for row in rows:
            url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyid/"+str(row.id)+"?sc=1"

            response = requests.request("GET", url, headers=init.headers)
            
            if response.status_code == 200:
                if response.text is not '':
                    
                    Fjson = json.loads(response.text)
                    
                    BrandName = Fjson["BrandName"]
                    UnitMultiplier = Fjson["UnitMultiplier"]
                    MeasurementUnit = Fjson["MeasurementUnit"]
                    EstimatedDateArrival = Fjson["EstimatedDateArrival"]
                    ProductGlobalCategoryId = Fjson["ProductGlobalCategoryId"]
                    AlternateIds = Fjson["AlternateIds"]
                    AlternateIds_RefId = AlternateIds["RefId"]
                    ModalType = Fjson["ModalType"]
                    RewardValue = Fjson["RewardValue"]
                    Images = Fjson["Images"]
                    for x in Images:
                        Images_ImageName = x["ImageName"]
                        Images_ImageUrl = x["ImageUrl"]
                        Images_FileId = x["FileId"]
                    ProductCategoryIds = Fjson["ProductCategoryIds"]
                    SkuSellers = Fjson["SkuSellers"]
                    for x in SkuSellers:
                        SkuSellers_StockKeepingUnitId = x["StockKeepingUnitId"]
                        SkuSellers_FreightCommissionPercentage = x["FreightCommissionPercentage"]
                        SkuSellers_ProductCommissionPercentage = x["ProductCommissionPercentage"]
                    Attachments_Id = None
                    RealDimension = Fjson["RealDimension"]
                    RealDimension_realWeight = RealDimension["realCubicWeight"]
                    CSCIdentification = Fjson["CSCIdentification"]
                    RealDimension_realLength = RealDimension["realLength"]
                    RealDimension_realWidth = RealDimension["realWidth"]
                    RealDimension_realCubicWeight = RealDimension["realCubicWeight"]    
                    SkuSellers = Fjson["SkuSellers"]
                    for x in SkuSellers:
                        SkuSellers_SellerId = x["SellerId"]
                        SkuSellers_IsActive = x["IsActive"]
                        SkuSellers_SellerStockKeepingUnitId = x["SellerStockKeepingUnitId"]
                    Attachments_Name = None
                    dimension_height = None
                    RealDimension_realHeight = None
                    ProductName = Fjson["ProductName"]
                    ImageUrl = Fjson["ImageUrl"]
                    DetailUrl = Fjson["DetailUrl"]
                    Attachments_IsRequired = None
                    ProductId = Fjson["ProductId"]
                    IsGiftCardRecharge = Fjson["IsGiftCardRecharge"]
                    NameComplete = Fjson["NameComplete"]
                    dimension_cubicweight = None
                    CommercialConditionId = Fjson["CommercialConditionId"]
                    IsTransported = Fjson["IsTransported"]
                    SkuName = Fjson["SkuName"]
                    Attachments_IsActive = None
                    dimension_length = None
                    InformationSource = Fjson["InformationSource"]
                    BrandId = Fjson["BrandId"]
                    IsActive = Fjson["IsActive"]
                    ProductDescription = Fjson["ProductDescription"]
                    IsInventoried = Fjson["IsInventoried"]
                    dimension_weight = None
                    Id  = Fjson["Id"]
                    
                    
                    df1 = pd.DataFrame({
                        'BrandName': BrandName,
                        'UnitMultiplier': UnitMultiplier,
                        'MeasurementUnit': MeasurementUnit,
                        'EstimatedDateArrival': EstimatedDateArrival,
                        'ProductGlobalCategoryId': ProductGlobalCategoryId,
                        'AlternateIds_RefId': AlternateIds_RefId,
                        'ModalType': ModalType,
                        'RewardValue': RewardValue,
                        'Images_ImageName': Images_ImageName,
                        'Images_ImageUrl': Images_ImageUrl,
                        'Images_FileId': Images_FileId,
                        'ProductCategoryIds': ProductCategoryIds,
                        'SkuSellers_StockKeepingUnitId': SkuSellers_StockKeepingUnitId,
                        'SkuSellers_FreightCommissionPercentage': SkuSellers_FreightCommissionPercentage,
                        'SkuSellers_ProductCommissionPercentage': SkuSellers_ProductCommissionPercentage,
                        'Attachments_Id': Attachments_Id,
                        'RealDimension_realWeight': RealDimension_realWeight,
                        'CSCIdentification': CSCIdentification,
                        'RealDimension_realLength': RealDimension_realLength,
                        'RealDimension_realWidth': RealDimension_realWidth,
                        'RealDimension_realCubicWeight': RealDimension_realCubicWeight,
                        'SkuSellers_SellerId': SkuSellers_SellerId,
                    '    SkuSellers_IsActive': SkuSellers_IsActive,
                        'SkuSellers_SellerStockKeepingUnitId': SkuSellers_SellerStockKeepingUnitId,
                        'Attachments_Name': Attachments_Name,
                    '    dimension_height': dimension_height,
                        'RealDimension_realHeight': RealDimension_realHeight,
                        'ProductName': ProductName,
                        'ImageUrl': ImageUrl,
                        'DetailUrl': DetailUrl,
                        'Attachments_IsRequired': Attachments_IsRequired,
                        'ProductId': ProductId,
                        'IsGiftCardRecharge': IsGiftCardRecharge,
                        'NameComplete': NameComplete,
                        'dimension_cubicweight': dimension_cubicweight,
                        'CommercialConditionId': CommercialConditionId,
                        'IsTransported': IsTransported,
                        'SkuName': SkuName,
                        'Attachments_IsActive': Attachments_IsActive,
                        'InformationSource': InformationSource,
                        'BrandId': BrandId,
                        'IsActive': IsActive,
                        'dimension_length': dimension_length,
                        'ProductDescription': ProductDescription,
                        'IsInventoried': IsInventoried,
                        'dimension_weight': dimension_weight,
                        'Id': Id}, index=[0])
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
        'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_sku_context` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_sku_context`')
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
            "name": "BrandName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "UnitMultiplier",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "MeasurementUnit",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "EstimatedDateArrival",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ProductGlobalCategoryId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "AlternateIds_RefId",
            "type": "STRING",
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
            "name": "Images_ImageName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "Images_ImageUrl",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "SkuSellers_ProductCommissionPercentage",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "ProductCategoryIds",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "SkuSellers_StockKeepingUnitId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "SkuSellers_FreightCommissionPercentage",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "Attachments_Id",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "RealDimension_realWeight",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "CSCIdentification",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "RealDimension_realLength",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "RealDimension_realWidth",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "RealDimension_realCubicWeight",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "SkuSellers_SellerId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "Attachments_Name",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "dimension_height",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "RealDimension_realHeight",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "Images_FileId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ProductName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ImageUrl",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "DetailUrl",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "Attachments_IsRequired",
            "type": "STRING",
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
            "name": "SkuSellers_IsActive",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "NameComplete",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "dimension_cubicweight",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "CommercialConditionId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "SkuSellers_SellerStockKeepingUnitId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "IsTransported",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "SkuName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "Attachments_IsActive",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "dimension_length",
            "type": "FLOAT",
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
            "name": "IsActive",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "ProductDescription",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "IsInventoried",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "dimension_weight",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "Id",
            "type": "INTEGER",
            "mode": "NULLABLE"
        }]
        
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_sku_context'
        
        
        
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
                #job_config.schema = format_schema(table_schema)
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