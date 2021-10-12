import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/collection/search"
querystring = {"page":"1","pageSize":"100","orderByAsc":"true"}
headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
response = requests.request("GET", url, headers=headers, params=querystring)
Json = json.loads(response.text)
paging = Json["paging"]
total = int(paging["total"])
pages = int(paging["pages"])
listItem = []
start = 0

def get_collection_beta(page,headers,total):
	url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/collection/search"
	querystring = {"page":""+str(page)+"","pageSize":""+str(total)+"","orderByAsc":"true"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	listItem.append(FJson["items"])
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/COLLECTION_BETA/items.json", "w")
	text_file.write(str(listItem))
	text_file.close()
	print("Pagina: "+str(page))



for x in range(pages):
	start += 1
	get_collection_beta(start,headers,total)
	break

system("cat items.json | jq -c '.[]' > tableCollectionBeta.json")

print("Cargando a BigQuery")
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






