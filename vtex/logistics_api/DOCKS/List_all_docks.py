import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

result = []

def get_list_docks():
	headers = {"Accept": "application/json; charset=utf-8","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	url = "https://mercury.vtexcommercestable.com.br/api/logistics/pvt/configuration/docks"
	querystring = {"total":"1"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/logistics_api/DOCKS/temp.json", "w")
	text_file.write(str(result))
	text_file.close()
	#cargando_bigquery()

def cargando_bigquery():
	print("Cargando a BigQuery")
	system("cat temp.json | jq -c '.[]' > listdocks.json")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/logistics_api/DOCKS/listdocks.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_list_docks'
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
	get_list_docks()




