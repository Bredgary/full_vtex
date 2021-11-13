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
	FROM = 0
	TO = 50
	df = pd.DataFrame()
	url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds"
	headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def get_productID(idCategory,From,To):
	querystring = {"categoryId":""+str(idCategory)+"","_from":""+str(From)+"","_to":""+str(To)+""}
	response = requests.request("GET", init.url, headers=init.headers, params=querystring)
	Fjson = json.loads(response.text)
	data = Fjson["data"]
	for x in data:
		if x is not "[]":
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
		querystring = {"categoryId":""+str(x)+""}
		response = requests.request("GET", init.url, headers=init.headers, params=querystring)
		FJson = json.loads(response.text)
		rango = FJson["range"]
		total = rango["total"]
		for y in range(total):
			get_productID(x,init.FROM,init.TO)
			init.FROM +=50
			init.TO += 50
			print(x)
			print(init.TO)
			if init.TO >= total:
				break


def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema


def run():
	get_params()
	for x in init.IDS:
		df1 = pd.DataFrame({'id': x}, index=[0])
		init.df = init.df.append(df1)

	df = init.df
	df.reset_index(drop=True, inplace=True)
	json_data = df.to_json(orient = 'records')
	json_object = json.loads(json_data)
	#print(json_object)
	
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
	job_config.autodetect = True
	#job_config.schema = format_schema(table_schema)
	job = client.load_table_from_json(json_object, table, job_config = job_config)
	print(job.result())
	
	
run()

