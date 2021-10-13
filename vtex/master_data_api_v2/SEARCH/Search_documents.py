import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import count

client = bigquery.Client()
productList = []
count = 0
x = ""
num1=0
num2=1
rangoT = 10000
def get_search_documents(x,rango1,rango2):
	try:
		url = "https://mercury.vtexcommercestable.com.br/api/dataentities/CL/search"
		querystring = {"_fields":"id,firstName,lastName,email,accountId,accountName,dataEntityId","_where":"email is not null"}
		headers = {"Content-Type": "application/json","Accept": "application/vnd.vtex.ds.v10+json","REST-Range": "resources="+str(num1)+"-"+str(num2)+"","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
		response = requests.request("GET", url, headers=headers, params=querystring)
		FJson = json.loads(response.text)
		result = json.dumps(FJson)
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/master_data_api_v2/SEARCH/temp.json", "w")
		text_file.write(result)
		text_file.close()
		cargando_bigquery()
	except:
		print("Vacio")

def cargando_bigquery():
	system("cat temp.json | jq -c '.[]' > search_documents.json")
	print("Cargando a BigQuery")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/master_data_api_v2/SEARCH/search_documents.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_search_documents'
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

def operacion_fenix(x,num1,num2):
	for x in range(rangoT):
		get_search_documents(x,num1,num2)
		num1 +=1
		num2 +=1
		print("Rango del: "+str(num1)+" al "+str(num2))

operacion_fenix(x,num1,num2)



