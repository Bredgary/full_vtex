#!/usr/bin/python
# -*- coding: latin-1 -*-
import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from os.path import join
import logging
from datetime import date
import datetime

class init:
  df = pd.DataFrame()
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
  dt = datetime.datetime.today()
  year = dt.year
  month = dt.month
  day = dt.day


def sku():
    try:
        print("Cargando consulta")
        client = bigquery.Client()
        QUERY = ('SELECT distinct productId, lastChange FROM `shopstar-datalake.staging_zone.shopstar_order_items`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        registro = 0
        for row in rows:
            url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitByProductId/"+str(row.productId)+""
            response = requests.request("GET", url, headers=init.headers)
            Fjson = json.loads(response.text)
            if Fjson is not "Sku não encontrado.":
                for x in Fjson:
                    print(Fjson)
                    IsPersisted = x["IsPersisted"]
                    Id = x["Id"]
                    ProductId = x["ProductId"]
                    IsActive = x["IsActive"]
                    Name = x["Name"]
                    Height = x["Height"]
                    RealHeight = x["RealHeight"]
                    Width = x["Width"]
                    RealWidth = x["RealWidth"]
                    Length = x["Length"]
                    RealLength = x["RealLength"]
                    WeightKg = x["WeightKg"]
                    RealWeightKg = x["RealWeightKg"]
                    ModalId = x["ModalId"]
                    RefId = x["RefId"]
                    CubicWeight = x["CubicWeight"]
                    IsKit = x["IsKit"]
                    InternalNote = x["InternalNote"]
                    DateUpdated = x["DateUpdated"]
                    RewardValue = x["RewardValue"]
                    CommercialConditionId = x["CommercialConditionId"]
                    EstimatedDateArrival = x["EstimatedDateArrival"]
                    FlagKitItensSellApart = x["FlagKitItensSellApart"]
                    ManufacturerCode = x["ManufacturerCode"]
                    ReferenceStockKeepingUnitId = x["ReferenceStockKeepingUnitId"]
                    Position = x["Position"]
                    ActivateIfPossible = x["ActivateIfPossible"]
                    MeasurementUnit = x["MeasurementUnit"]
                    UnitMultiplier = x["UnitMultiplier"]
                    IsInventoried = x["IsInventoried"]
                    IsTransported = x["IsTransported"]
                    IsGiftCardRecharge = x["IsGiftCardRecharge"]
                    ModalType = x["ModalType"]
                    isKitOptimized = x["isKitOptimized"]
                    df1 = pd.DataFrame({
                        'IsPersisted': IsPersisted,
                        'SkuId': Id,
                        'ProductId': ProductId,
                        'IsActive': IsActive,
                        'Name': Name,
                        'Height': Height,
                        'RealHeight': RealHeight,
                        'Width': Width,
                        'RealWidth': RealWidth,
                        'Length': Length,
                        'RealLength': RealLength,
                        'WeightKg': WeightKg,
                        'RealWeightKg': RealWeightKg,
                        'ModalId': ModalId,
                        'RefId': RefId,
                        'CubicWeight': CubicWeight,
                        'IsKit': IsKit,
                        'InternalNote': InternalNote,
                        'DateUpdated': DateUpdated,
                        'RewardValue': RewardValue,
                        'CommercialConditionId': CommercialConditionId,
                        'EstimatedDateArrival': EstimatedDateArrival,
                        'FlagKitItensSellApart': FlagKitItensSellApart,
                        'ManufacturerCode': ManufacturerCode,
                        'ReferenceStockKeepingUnitId': ReferenceStockKeepingUnitId,
                        'Position': Position,
                        'ActivateIfPossible': ActivateIfPossible,
                        'MeasurementUnit': MeasurementUnit,
                        'UnitMultiplier': UnitMultiplier,
                        'IsInventoried': IsInventoried,
                        'IsTransported': IsTransported,
                        'IsGiftCardRecharge': IsGiftCardRecharge,
                        'ModalType': ModalType,
                        'isKitOptimized': isKitOptimized}, index=[0])
                    init.df = init.df.append(df1)
                    registro += 1
                    print("Registro: "+str(registro))
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
        'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_sku` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_sku`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    print(rows)

def run():
    try:
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        

        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_sku'
        
        if df.empty:
            print('DataFrame is empty!')
        else:
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
        
sku()
