import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

listaEan = []
f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/delimitador.txt','r')
data_from_string = f_01.read()
delimitador = int(data_from_string)
count = 0

def get_ean(id,count,delimitador):
	if count >= delimitador:
		#try:
		url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"+str(id)+"/ean"
		headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
		response = requests.request("GET", url, headers=headers)
		temp = response.text
		idEan = temp.replace("[", "{id:").replace("]","}")
		listaEan.append(idEan)
		print("Num: "+str(count))
			#cargando_bigquery()
		#except:
		#	delimitador = count + 1                 
		#	text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/delimitador.txt", "w")
		#	text_file.write(str(delimitador))
		#	text_file.close()
		#	system("python3 Get_EAN_by_SkuId.py")


def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/id_sku.json','r')
	data_from_string = f_01.read()
	data_from_string = data_from_string.replace('"', '')
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		count += 1
		get_ean(i,count,delimitador)
	print(str(count)+" registro almacenado.")


def cargando_bigquery():
	print("Cargando a BigQuery")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/sku_ean.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_sku_ean_id'
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

operacion_fenix(count)


text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/table_ean_f.txt", "w")
text_file.write(str(listaEan))
text_file.close(listaEan)
