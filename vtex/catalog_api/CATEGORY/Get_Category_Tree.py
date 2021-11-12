import pandas as pd
import numpy as np
import requests
import json
import os
import re
import datetime
from datetime import date
from datetime import timedelta
from os import system
from google.cloud import bigquery
import logging

class Init:
	lista = []

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def get_order_list():
	url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pub/category/tree/100000"
	headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers)
	FJson = json.loads(response.text)
	return FJson


def dataframe(raiz, isChildren):
	if isChildren:
		for x in raiz:
			print(x)
		hasChildren = son["hasChildren"]
		print(hasChildren)
		#dataframe(son, hasChildren)
	else:
		for x in raiz:
			print(x)
		hasChildren = son["hasChildren"]
		print(hasChildren)
		#dataframe(son, hasChildren)
	return Init.df



def run():
	FJson = get_order_list()
	for x in FJson:
		hasChildren = x["hasChildren"]
		dataframe(FJson,hasChildren)
		
'''
	df.reset_index(drop=True, inplace=True)
	json_data = df.to_json(orient = 'records')
	json_object = json.loads(json_data)

	table_schema = {
		"name": "id",
		"type": "INTEGER",
		"mode": "NULLABLE"
		},{
			"name": "name",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "hasChildren",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},{
			"name": "url",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "Title",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "MetaTagDescription",
			"type": "STRING",
			"mode": "NULLABLE"}

	
	project_id = '999847639598'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_category_test'
	
	client  = bigquery.Client(project = project_id)
	table = dataset.table(table_id)
	job_config = bigquery.LoadJobConfig()
	job_config.write_disposition = "WRITE_TRUNCATE"
	job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
	job_config.schema = format_schema(table_schema)
	job = client.load_table_from_json(json_object, table, job_config = job_config)
	print(job.result())
	'''
run()

