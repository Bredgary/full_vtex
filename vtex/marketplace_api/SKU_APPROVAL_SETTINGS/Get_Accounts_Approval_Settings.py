import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

client = bigquery.Client()

def get_accounts_approval_settings():
	#try:
	url = "https://api.vtex.com/mercury/suggestions/configuration"
	headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers)
	FJson = json.loads(response.text)
	result = json.dumps(FJson)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/marketplace_api/SKU_APPROVAL_SETTINGS/temp.json", "w")
	text_file.write(result)
	text_file.close()
	cargando_bigquery()
	#except:
	#	print("Error")

def cargando_bigquery():
	#try:
	print("Cargando a BigQuery")
	#system("cat temp.json | jq -c '.[]' > table_shipping_policies.json")
	filename = '/home/bred_valenzuela/full_vtex/vtex/marketplace_api/SKU_APPROVAL_SETTINGS/items.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_accounts_approval_settings'
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
	#except:
	#	print("Error")

for x in range(1):
	get_accounts_approval_settings()
