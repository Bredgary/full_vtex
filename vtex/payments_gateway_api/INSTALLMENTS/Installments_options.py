import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict


def installments():
	url = "https://mercury.vtexpayments.com.br/api/pvt/installments"
	querystring = {"request.value":"10","request.salesChannel":"1","request.paymentDetails[0].id":"2","request.paymentDetails[0].value":"10","request.paymentDetails[0].bin":"411111"}
	headers = {"Accept": "application/json; charset=utf-8","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	result = json.dumps(FJson)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/payments_gateway_api/INSTALLMENTS/items2.json", "w")
	text_file.write(result)
	text_file.close()
	cargando_bigquery()

def cargando_bigquery():
	print("Cargando a BigQuery")
	#system("cat items.json | jq -c '.[]' > tableInstallments.json")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/payments_gateway_api/INSTALLMENTS/items.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_installments'
	dataset_ref = client.dataset(dataset_id)
	table_ref = dataset_ref.table(table_id)
	job_config = bigquery.LoadJobConfig()
	job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
	job_config.autodetect = True
	with open(filename, "rb") as source_file:
		job = client.load_table_from_file(
			source_file,
			table_ref,
			location="southamerica-east1",  # Must match the destination dataset location.
		job_config=job_config,)  # API request
	job.result()  # Waits for table load to complete.
	print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
	print("finalizado")

installments()

'''
QUERY = (
    'SELECT FieldId FROM `shopstar-datalake.landing_zone.shopstar_vtex_sku_specification` WHERE FieldId is not null')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
    productList.append(row.FieldId)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SPECIFICATION_FIELD/SPECIFICATION_FIELD_ID_2.json", "w")
text_file.write(string)
text_file.close()
'''
