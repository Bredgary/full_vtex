#!/usr/bin/python
# -*- coding: UTF-8 -*-
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

def getListSpecificationsGroupByCategory(id,reg):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/specification/groupbycategory/"+str(id)+""
    response = requests.request("GET", url, headers=init.headers)
    Fjson = json.loads(response.text)
    for x in Fjson:
    	df1 = pd.DataFrame({'id': x["Id"],'name': x["Name"],'position': x["Position"],'categoryId': x["CategoryId"]}, index=[0])
    	init.df = init.df.append(df1)
    print("Recorriendo tabla category: "+str(reg))

def get_params():
	print("Cargando consulta")
	client = bigquery.Client()
	QUERY = ('SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_category`')
	query_job = client.query(QUERY)
	rows = query_job.result()
	registro = 0
	for row in rows:
		registro += 1
		getListSpecificationsGroupByCategory(row.id,registro)
		

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def delete_duplicate():
    client = bigquery.Client()
    QUERY = (
        'CREATE OR REPLACE TABLE `shopstar-datalake.landing_zone.shopstar_vtex_list_group_specifications` AS SELECT DISTINCT * FROM `shopstar-datalake.landing_zone.shopstar_vtex_list_group_specifications`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    print(rows)

def run():
    get_params()
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    table_schema = [{
		"name": "CategoryId",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "Id",
		"type": "INTEGER",
		"mode": "NULLABLE"
	},{
		"name": "Name",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "Position",
		"type": "INTEGER",
		"mode": "NULLABLE"
	}]

    project_id = '999847639598'
    dataset_id = 'landing_zone'
    table_id = 'shopstar_vtex_list_group_specifications'

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