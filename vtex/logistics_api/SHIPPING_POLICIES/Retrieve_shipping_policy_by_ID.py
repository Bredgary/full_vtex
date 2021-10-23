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
count = 0

def get_retrieve(id,count):
	url = "https://mercury.vtexcommercestable.com.br/api/logistics/pvt/shipping-policies/"+str(id)+""
	headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers)
	FJson = json.loads(response.text)
	result = json.dumps(FJson)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/logistics_api/SHIPPING_POLICIES/items.json", "w")
	text_file.write(result)
	text_file.close()
	cargando_bigquery(id)

def cargando_bigquery(id):
	try:
		print("Cargando a BigQuery")
		filename = '/home/bred_valenzuela/full_vtex/vtex/logistics_api/SHIPPING_POLICIES/items.json'
		dataset_id = 'landing_zone'
		table_id = 'shopstar_vtex_retrieve_shipping'
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
		print("Vacio: "+str(id))

def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/logistics_api/SHIPPING_POLICIES/shipping_id.json','r')
	data_from_string = f_01.read()
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		count += 1
		get_retrieve(i,count)
	print(str(count)+" registro almacenado.")

operacion_fenix(count)

'''
QUERY = (
    'SELECT distinct id FROM `shopstar-datalake.landing_zone.shopstar_vtex_list_shipping_policies`')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
    productList.append(row.id)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/logistics_api/SHIPPING_POLICIES/shipping_id.json", "w")
text_file.write(string)
text_file.close()
'''
