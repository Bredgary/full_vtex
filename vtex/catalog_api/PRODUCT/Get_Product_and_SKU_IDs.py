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
	IDS = []
	productList = []

def get_productID(idCategory,From,To):
	url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds"
	querystring = {"categoryId":""+str(idCategory)+"","_from":""+str(From)+"",""+str(To)+"":"54"}
	headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	Fjson = json.loads(response.text)
	data = Fjson["data"]
	for x in data:
		init.IDS.append(x)


def get_params():
	client = bigquery.Client()
	QUERY = (
		'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_category` where predecessor= 0')
	query_job = client.query(QUERY)  
	rows = query_job.result()
	for row in rows:
		init.productList.append(row.id)
	for x in init.productList:
		get_productID(x,1,50)


def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema


def run():
	get_params()
	print(init.IDS)
	'''
	for x in raiz:
		df1 = pd.DataFrame({
			'id': x["id"],
			'name': x["name"],
			'url': x["url"],
			'title': x["Title"],
			'metaTagDescription': x["MetaTagDescription"],
			'predecessor': 0,
			'hasChildren': str(x["hasChildren"])}, index=[0])
		init.df = init.df.append(df1)
		son = x["children"]
		children(son,x["id"])

	df = init.df
	df.reset_index(drop=True, inplace=True)
	json_data = df.to_json(orient = 'records')
	json_object = json.loads(json_data)

	table_schema = {
		"name": "id",
		"type": "INTEGER",
		"mode": "NULLABLE"
		}


	project_id = '999847639598'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_product_ID'

	client  = bigquery.Client(project = project_id)
	dataset  = client.dataset(dataset_id)
	table = dataset.table(table_id)
	job_config = bigquery.LoadJobConfig()
	job_config.write_disposition = "WRITE_TRUNCATE"
	job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
	job_config.schema = format_schema(table_schema)
	job = client.load_table_from_json(json_object, table, job_config = job_config)
	print(job.result())
'''
run()

