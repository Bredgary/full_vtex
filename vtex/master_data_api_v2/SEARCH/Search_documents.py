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

'''
def get_subcollection(id):
	try:
		url = "https://mercury.vtexcommercestable.com.br/api/dataentities/CL/search"
		querystring = {"_fields":"id,firstName,lastName,email,accountId,accountName,dataEntityId","_where":"email is not null"}
		headers = {"Content-Type": "application/json","Accept": "application/vnd.vtex.ds.v10+json","REST-Range": "resources=0-1","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
		response = requests.request("GET", url, headers=headers, params=querystring)
		FJson = json.loads(response.text)
		result = json.dumps(FJson)
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SUB_COLLECTION/temp.json", "w")
		text_file.write(result)
		text_file.close()
		cargando_bigquery()
	except:
		print("Vacio")

def cargando_bigquery():
	system("cat temp.json | jq -c '.[]' > sub_collection.json")
	print("Cargando a BigQuery")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SUB_COLLECTION/sub_collection.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_sub_collection'
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

def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SUB_COLLECTION/id_collecion.json','r')
	data_from_string = f_01.read()
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		get_subcollection(i)
		count += 1
		print(str(count)+" registro almacenado.")

operacion_fenix(count)
'''

for i in count(0):
	print("Hola")

