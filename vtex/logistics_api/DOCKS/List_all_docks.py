import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

url = "https://mercury.vtexcommercestable.com.br/api/logistics/pvt/configuration/docks"
querystring = {"page":"1","perPage":"1"}
headers = {"Accept": "application/json; charset=utf-8","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
response = requests.request("GET", url, headers=headers, params=querystring)
Json = json.loads(response.text)
paging = Json["paging"]
total = int(paging["total"])
pages = int(paging["pages"])
start = 0

def get_list_docks(start,headers):
	url = "https://mercury.vtexcommercestable.com.br/api/logistics/pvt/configuration/docks"
	querystring = {"page":""+str(start)+"","perPage":"1"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	result = json.dumps(FJson["items"])
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/logistics_api/DOCKS/temp.json", "w")
	text_file.write(result)
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

for x in range(pages):
	start += 1
	get_list_docks(start,headers)




