import pandas as pd
import numpy as np
import requests
import json
import os
import re
import datetime
from datetime import timedelta, timezone, date
from os import system
from google.cloud import bigquery
import logging

naive_dt = datetime.now()
aware_dt = naive_dt.astimezone()
# correct, ISO-8601 (but not UTC)
aware_dt.isoformat(timespec="seconds")
# lets get the time in UTC
utc_dt = aware_dt.astimezone(timezone.utc)
# correct, ISO-8601 and UTC (but not in UTC format)
date_str = utc_dt.isoformat(timespec='milliseconds')
date = date_str.replace("+00:00", "Z")


class Init:
	today = datetime.date.today()
	yesterday = today - datetime.timedelta(days=1)
	before_yesterday = today - datetime.timedelta(days=2)
	ordenes = {}
	df = pd.DataFrame()

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def get_order_list(page):
	url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders?page="+str(page)+""
	querystring = {"f_creationDate":"creationDate:["+str(Init.before_yesterday)+"T02:00:00.000Z TO "+str(Init.yesterday)+"T01:59:59.999Z]","f_hasInputInvoice":"false"}
	headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	return FJson

def paging():
	for x in range(30):
		FJson = get_order_list(x)
		if FJson["list"]:
			Init.ordenes.update(FJson)
		else:
			break
	return Init.ordenes

def dataframe():
	print("Cargando Dataframe")
	FJson = paging()
	lista = FJson["list"]
	for x in lista:
		df1 = pd.DataFrame({
			'orderId': str(x["orderId"]),
			'creationDate': str(x["creationDate"]),
			'clientName': str(x["clientName"]),
			'totalValue': str(x["totalValue"]),
			'paymentNames': str(x["paymentNames"]),
			'status': str(x["status"]),
			'statusDescription': str(x["statusDescription"]),
			'marketPlaceOrderId': str(x["marketPlaceOrderId"]),
			'sequence': str(x["sequence"]),
			'salesChannel': str(x["salesChannel"]),
			'affiliateId': str(x["affiliateId"]),
			'origin': str(x["origin"]),
			'workflowInErrorState': str(x["workflowInErrorState"]),
			'workflowInRetry': str(x["workflowInRetry"]),
			'lastMessageUnread': str(x["lastMessageUnread"]),
			'ShippingEstimatedDate': str(x["ShippingEstimatedDate"]),
			'ShippingEstimatedDateMax': str(x["ShippingEstimatedDateMax"]),
			'ShippingEstimatedDateMin': str(x["ShippingEstimatedDateMin"]),
			'orderIsComplete': str(x["orderIsComplete"]),
			'listId': str(x["listId"]),
			'listType': str(x["listType"]),
			'authorizedDate': str(x["authorizedDate"]),
			'callCenterOperatorName': str(x["callCenterOperatorName"]),
			'totalItems': str(x["totalItems"]),
			'currencyCode': str(x["currencyCode"])}, index=[0])
		Init.df = Init.df.append(df1)
	return Init.df

def delete_duplicate():
	try:
		print("Eliminando duplicados")
		client = bigquery.Client()
		QUERY = (
			'CREATE OR REPLACE TABLE `shopstar-datalake.landing_zone.shopstar_vtex_list_order` AS SELECT DISTINCT * FROM `shopstar-datalake.landing_zone.shopstar_vtex_list_order`')
		query_job = client.query(QUERY)
		rows = query_job.result()
		print(rows)
	except:
		print("Consulta SQL no ejecutada")


def run():
	df = dataframe()
	df.reset_index(drop=True, inplace=True)
	json_data = df.to_json(orient = 'records')
	json_object = json.loads(json_data)
	
	table_schema = {
			"name": "orderId",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "creationDate",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "clientName",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "totalValue",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "paymentNames",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "status",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "statusDescription",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "marketPlaceOrderId",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "sequence",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "salesChannel",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "affiliateId",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "origin",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "workflowInErrorState",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "workflowInRetry",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "lastMessageUnread",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "ShippingEstimatedDate",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "ShippingEstimatedDateMax",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "ShippingEstimatedDateMin",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "orderIsComplete",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "listId",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "listType",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "authorizedDate",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "callCenterOperatorName",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "totalItems",
			"type": "STRING",
			"mode": "NULLABLE"
		},{
			"name": "currencyCode",
			"type": "STRING",
			"mode": "NULLABLE"}
	
	project_id = '999847639598'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_list_order_t'
	table_temp = 'order_write'
	
	client  = bigquery.Client(project = project_id)
	dataset  = client.dataset(dataset_id)
	tableO = dataset.table(table_id)
	job_config = bigquery.LoadJobConfig()
	job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
	job_config.schema = format_schema(table_schema)
	job = client.load_table_from_json(json_object, tableO, job_config = job_config)
	print(job.result())
	
	tableT = dataset.table(table_temp)
	job_config_temp = bigquery.LoadJobConfig()
	job_config_temp.write_disposition = "WRITE_TRUNCATE"
	job_config_temp.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
	job_config_temp.schema = format_schema(table_schema)
	job = client.load_table_from_json(json_object, tableT, job_config = job_config_temp)
	print(job.result())

run()



