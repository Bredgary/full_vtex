import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

productList =[]
client = bigquery.Client()

'''
url = "https://mercury.vtexcommercestable.com.br/api/license-manager/site/pvt/logins/list/paged"
querystring = {"numItems":"10","pageNumber":"1","sort":"name","sortType":"ASC"}
headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
response = requests.request("GET", url, headers=headers, params=querystring)
Json = json.loads(response.text)
paging = Json["paging"]
total = int(paging["total"])
pages = int(paging["pages"])
listItem = []
start = 0

def get_user(page,headers,total):
	try:
		url = "https://mercury.vtexcommercestable.com.br/api/license-manager/site/pvt/logins/list/paged"
		querystring = {"numItems":""+str(page)+"","pageNumber":""+str(total)+"","sort":"name","sortType":"ASC"}
		response = requests.request("GET", url, headers=headers, params=querystring)
		FJson = json.loads(response.text)
		result = json.dumps(FJson["items"])
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/license_manager_api/USER/items.json", "w")
		text_file.write(result)
		text_file.close()
		print("Pagina: "+str(page))
		cargando_bigquery()
	except:
		print("Error")

def cargando_bigquery():
	try:
		print("Cargando a BigQuery")
		system("cat items.json | jq -c '.[]' > table_user.json")
		filename = '/home/bred_valenzuela/full_vtex/vtex/license_manager_api/USER/table_user.json'
		dataset_id = 'landing_zone'
		table_id = 'shopstar_vtex_user_detail'
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
	except:
		print("Error")

for x in range(pages):
	start += 1
	get_user(start,headers,total)

'''
QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_user`')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
    productList.append(row.id)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/license_manager_api/USER/user_id.json", "w")
text_file.write(string)
text_file.close()
