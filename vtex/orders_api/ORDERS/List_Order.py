import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

def get_order_list():
	url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders?f_creationDate=creationDate:[2020-01-01T02:00:00.000Z TO 2021-010-01T01:59:59.999Z]"
	querystring = {"f_hasInputInvoice":"false","pages":"4927","total":"73902"}
	headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	result = json.dumps(FJson["list"])
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/list.json", "w")
	text_file.write(result)
	text_file.close()
	cargando_bigquery()

def cargando_bigquery():
	print("Cargando a BigQuery")
	system("cat list.json | jq -c '.[]' > list_table.json")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/list_table.json'
	dataset_id = 'landing_zone'
	table_id = 'test_list_order'
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

for x in range(1):
	get_order_list()

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
