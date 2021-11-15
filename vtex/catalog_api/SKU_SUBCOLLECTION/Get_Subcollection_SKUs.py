import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
productList = []
count = 0

def get_aen(id):
	try:
		url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/subcollection/"+str(id)+"/stockkeepingunit"
		querystring = {"page":"1","size":"10000"}
		headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
		response = requests.request("GET", url, headers=headers, params=querystring)
		FJson = json.loads(response.text)
		result = json.dumps(FJson)
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_SUBCOLLECTION/temp.json", "w")
		text_file.write(result)
		text_file.close()
		cargando_bigquery()
	except:
		print("Vacio")

def cargando_bigquery():
	#system("cat temp.json | jq -c '.[]' > eat_table.json")
	print("Cargando a BigQuery")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_SUBCOLLECTION/temp.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_sku_sub_collection'
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
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_SUBCOLLECTION/id_sub_collection.json','r')
	data_from_string = f_01.read()
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		get_aen(i)
		count += 1
		#print(str(count)+" registro almacenado.")

operacion_fenix(count)


'''
QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_sub_collection`')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    productList.append(row.id)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_SUBCOLLECTION/id_sub_collection.json", "w")
text_file.write(string)
text_file.close()
'''