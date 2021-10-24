import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

client = bigquery.Client()
productList = []
count=0

def list_inventory_by_sku(id,count):
	url = "https://mercury.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/"+str(id)+""
	headers = {"Accept": "application/json; charset=utf-8","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers)
	FJson = json.loads(response.text)
	result = json.dumps(FJson["balance"])
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/logistics_api/INVENTORY/temp.json", "w")
	text_file.write(result)
	text_file.close()
	print("Registro: "+str(count))
	if count >= 558:
		cargando_bigquery(result,count)

def cargando_bigquery(result,count):
	try:
		print("Cargando a BigQuery")
		system("cat temp.json | jq -c '.[]' > list_inventory.json")
		client = bigquery.Client()
		filename = '/home/bred_valenzuela/full_vtex/vtex/logistics_api/INVENTORY/list_inventory.json'
		dataset_id = 'landing_zone'
		table_id = 'shopstar_vtex_list_inventory_by_sku'
		dataset_ref = client.dataset(dataset_id)
		table_ref = dataset_ref.table(table_id)
		job_config = bigquery.LoadJobConfig()
		job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
		#job_config.autodetect = True
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
		print("Vacio")
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/logistics_api/INVENTORY/almacenLocal/"+str(count)+"_regstro.json", "w")
		text_file.write(result)
		text_file.close()

def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/logistics_api/INVENTORY/id_sku.json','r')
	data_from_string = f_01.read()
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		count +=1
		list_inventory_by_sku(i,count)

operacion_fenix(count)

'''
QUERY = (
    'SELECT Id FROM `shopstar-datalake.landing_zone.shopstar_vtex_sku`')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
    productList.append(row.Id)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/logistics_api/INVENTORY/id_sku.json", "w")
text_file.write(string)
text_file.close()
'''






