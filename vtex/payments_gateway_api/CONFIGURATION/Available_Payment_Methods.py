import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict


def payment_systems():
	url = "https://mercury.vtexpayments.com.br/api/pvt/merchants/payment-systems"
	headers = {"Accept": "application/json; charset=utf-8","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}response = requests.request("GET", url, headers=headers)
	FJson = json.loads(response.text)
	result = json.dumps(FJson)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/payments_gateway_api/CONFIGURATION/items.json", "w")
	text_file.write(result)
	text_file.close()
	cargando_bigquery()

def cargando_bigquery():
	print("Cargando a BigQuery")
	system("cat items.json | jq -c '.[]' > payment_systems.json")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/payments_gateway_api/CONFIGURATION/payment_systems.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_payment_systems'
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

payment_systems()