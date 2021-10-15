import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

productList = []
client = bigquery.Client()
count = 0

def value(id,count):
	try:
		url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/specificationvalue/"+str(id)+""
		headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
		response = requests.request("GET", url, headers=headers)
		FJson = json.loads(response.text)
		result = json.dumps(FJson)
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SPECIFICATION_VALUE/value.json", "w")
		text_file.write(result)
		text_file.close()
		print("Registro: "+str(count))
		cargando_bigquery()
	except:
		print("Vacio")


def cargando_bigquery():
	print("Cargando a BigQuery")
	system("cat value.json | jq -c '.[]' > value_table.json")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SPECIFICATION_VALUE/value_table.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_specification_value'
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
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SPECIFICATION_VALUE/SPECIFICATION_VALUE.json','r')
	data_from_string = f_01.read()
	data_from_string = data_from_string.replace('"', '')
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		count += 1
		value(i,count)
	print(str(count)+" registro almacenado.")

operacion_fenix(count)

'''
QUERY = (
    'SELECT FieldValueId FROM `shopstar-datalake.landing_zone.shopstar_vtex_field_value` where FieldValueId is not null')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
    productList.append(row.FieldValueId)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SPECIFICATION_VALUE/SPECIFICATION_VALUE.json", "w")
text_file.write(string)
text_file.close()
'''


