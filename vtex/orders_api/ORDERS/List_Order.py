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

class Init:
	today = datetime.date.today()
	yesterday = today - datetime.timedelta(days=1)
	before_yesterday = today - datetime.timedelta(days=2)
	ordenes = {}

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

def run():
	FJson = paging()
	lista = FJson["list"]
	print(type(lista))

	'''
    df = pd.DataFrame({
		'orderId': FJson["orderId"]}, index=[0])

    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    print(json_object)
    
    table_schema = [
		{
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
			"mode": "NULLABLE"
		}]

    project_id = '999847639598'
    dataset_id = 'landing_zone'
    table_id = 'shopstar_vtex_current_cart'

    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = format_schema(table_schema)
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
	'''
	
run()



