#!/usr/bin/python
# -*- coding: latin-1 -*-
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

def get_product(id,reg):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        init.productList.append(Fjson)
        print("Registro: "+str(reg))
    except:
        print("Vacio")

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_SKU_ID`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 1
    for row in rows:
        get_product(row.id,registro)
        registro += 1
        if registro == 15:
        	break
    

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def delete_duplicate():
    client = bigquery.Client()
    QUERY = (
        'CREATE OR REPLACE TABLE `shopstar-datalake.landing_zone.shopstar_vtex_sku` AS SELECT DISTINCT * FROM `shopstar-datalake.landing_zone.shopstar_vtex_sku`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    print(rows)

def run():
    get_params()
    
    for x in init.productList:
        df1 = pd.DataFrame({
            'id': x["Id"],
            'productId': x["ProductId"],
            'isActive': x["IsActive"],
            'activateIfPossible': x["ActivateIfPossible"],
            'name': x["Name"],
            'refId': x["RefId"],
            'packagedHeight': x["PackagedHeight"],
            'packagedLength': x["PackagedLength"],
            'packagedWidth': x["PackagedWidth"],
            'packagedWeightKg': x["PackagedWeightKg"],
            'height': x["Height"],
            'length': x["Length"],
            'width': x["Width"],
            'weightKg': x["WeightKg"],
            'cubicWeight': x["CubicWeight"],
            'isKit': x["IsKit"],
            'creationDate': x["CreationDate"],
            'rewardValue': x["RewardValue"],
            'estimatedDateArrival': x["EstimatedDateArrival"],
            'manufacturerCode': x["ManufacturerCode"],
            'commercialConditionId': x["CommercialConditionId"],
            'measurementUnit': x["MeasurementUnit"],
            'unitMultiplier': x["UnitMultiplier"],
            'modalType': x["ModalType"],
            'videos': x["Videos"]}, index=[0])
        init.df = init.df.append(df1)

    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    table_schema = [
		{
			"name": "Id",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},{
			"name": "ProductId",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},{
			"name": "IsActive",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},{
			"name": "ActivateIfPossible",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},{
			"name": "Name",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "RefId",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},{
			"name": "PackagedHeight",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},{
			"name": "PackagedLength",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},{
			"name": "PackagedWidth",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},{
			"name": "PackagedWeightKg",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},{
			"name": "Height",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "Length",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "Width",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "WeightKg",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "CubicWeight",
			"type": "FLOAT",
			"mode": "NULLABLE"
		},{
			"name": "IsKit",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},{
			"name": "CreationDate",
			"type": "DATE",
			"mode": "NULLABLE"
		},{
			"name": "RewardValue",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "EstimatedDateArrival",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "ManufacturerCode",
			"type": "FLOAT",
			"mode": "NULLABLE"
		},{
			"name": "CommercialConditionId",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},{
			"name": "MeasurementUnit",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "UnitMultiplier",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},{
			"name": "ModalType",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "KitItensSellApart",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},{
			"name": "Videos",
			"type": "STRING",
			"mode": "REPEATED"
		}]
    
    project_id = '999847639598'
    dataset_id = 'landing_zone'
    table_id = 'shopstar_vtex_sku_test'

    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = format_schema(table_schema)
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()
    
run()