import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict


fromD = 1
toD = 2
page = 1

def get_order_list(fromD,toD,page):
	for x in range(30):
		url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders?page="+str(page)+""
		querystring = {"f_creationDate":"creationDate:[2020-01-"+str(fromD)+"T02:00:00.000Z TO 2020-01-"+str(toD)+"T01:59:59.999Z]","f_hasInputInvoice":"false"}
		headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
		response = requests.request("GET", url, headers=headers, params=querystring)
		FJson = json.loads(response.text)
		res = FJson['list']
		if not res:
			break
		result = json.dumps(res)
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/list.json", "w")
		text_file.write(result)
		text_file.close() 
		print("Pagina: "+str(page))
		cargando_bigquery()
		page += 1

def cargando_bigquery():
	print("Cargando a BigQuery")
	system("cat list.json | jq -c '.[]' > list_table.json")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/list_table.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_list_order_v1'
	dataset_ref = client.dataset(dataset_id)
	table_ref = dataset_ref.table(table_id)
	job_config = bigquery.LoadJobConfig(
		schema=[
        bigquery.SchemaField("orderId", "STRING"),
        bigquery.SchemaField("creationDate", "DATE"),
		bigquery.SchemaField("clientName", "STRING"),
		bigquery.SchemaField("items", "STRING"),
		bigquery.SchemaField("totalValue", "INTEGER"),
		bigquery.SchemaField("paymentNames", "STRING"),
		bigquery.SchemaField("status", "STRING"),
		bigquery.SchemaField("statusDescription", "STRING"),
		bigquery.SchemaField("marketPlaceOrderId", "STRING"),
		bigquery.SchemaField("sequence", "INTEGER"),
		bigquery.SchemaField("salesChannel", "INTEGER"),
		bigquery.SchemaField("affiliateId", "FLOAT"),
		bigquery.SchemaField("origin", "STRING"),
		bigquery.SchemaField("workflowInErrorState", "BOOLEAN"),
		bigquery.SchemaField("workflowInRetry", "BOOLEAN"),
		bigquery.SchemaField("lastMessageUnread", "STRING"),
		bigquery.SchemaField("ShippingEstimatedDate", "STRING"),
		bigquery.SchemaField("ShippingEstimatedDateMax", "DATE"),
		bigquery.SchemaField("ShippingEstimatedDateMin", "DATE"),
		bigquery.SchemaField("orderIsComplete", "BOOLEAN"),
		bigquery.SchemaField("listId", "STRING"),
		bigquery.SchemaField("listType", "STRING"),
		bigquery.SchemaField("authorizedDate", "DATE"),
		bigquery.SchemaField("callCenterOperatorName", "STRING"),
		bigquery.SchemaField("totalItems", "INTEGER"),
		bigquery.SchemaField("currencyCode", "STRING"),
		bigquery.SchemaField("hostname", "STRING"),
		bigquery.SchemaField("invoiceOutput", "STRING"),
		bigquery.SchemaField("invoiceInput", "STRING"),
		bigquery.SchemaField("lastChange", "DATE"),
		bigquery.SchemaField("isAllDelivered", "BOOLEAN"),
		bigquery.SchemaField("isAnyDelivered", "BOOLEAN"),
		bigquery.SchemaField("giftCardProviders", "STRING"),
		bigquery.SchemaField("orderFormId", "STRING"),
		bigquery.SchemaField("paymentApprovedDate", "STRING"),
		bigquery.SchemaField("readyForHandlingDate", "STRING"),
		bigquery.SchemaField("deliveryDates", "STRING"),
    ],
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
	)
	#job_config.autodetect = True
	with open(filename, "rb") as source_file:
		job = client.load_table_from_file(
			source_file,
			table_ref,
			location="southamerica-east1",  # Must match the destination dataset location.
		job_config=job_config,)  # API request
	job.result()  # Waits for table load to complete.
	print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
	print("finalizado")

for x in range(31):
	get_order_list(fromD,toD,page)
	fromD += 1
	toD += 1

