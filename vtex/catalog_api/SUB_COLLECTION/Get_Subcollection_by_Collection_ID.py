import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
productList = []
'''

def get_collection_beta(page,headers,total):
	url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/collection/search"
	querystring = {"page":""+str(page)+"","pageSize":""+str(total)+"","orderByAsc":"true"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	result = json.dumps(FJson["items"])
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/COLLECTION_BETA/items.json", "w")
	text_file.write(result)
	text_file.close()
	print("Pagina: "+str(page))
	cargando_bigquery()

def cargando_bigquery():
	print("Cargando a BigQuery")
	system("cat items.json | jq -c '.[]' > tableCollectionBeta.json")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/COLLECTION_BETA/tableCollectionBeta.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_collection_beta'
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
	system("rm items.json")
	system("rm tableCollectionBeta.json")

for x in range(pages):
	start += 1
	get_collection_beta(start,headers,total)
'''

QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_collection_beta`')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    productList.append(row.id)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SUB_COLLECTION//id_collecion.json", "w")
text_file.write(string)
text_file.close()

