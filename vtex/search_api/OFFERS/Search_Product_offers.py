#!/usr/bin/python
# -*- coding: latin-1 -*-
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

def search_product_offers(id,count):
	try:
		url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pub/products/offers/"+str(id)+""
		headers = {"Accept": "application/json; charset=utf-8","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
		response = requests.request("GET", url, headers=headers)
		FJson = json.loads(response.text)
		result = json.dumps(FJson)
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/search_api/OFFERS/itemsP.json", "w")
		text_file.write(result)
		text_file.close()
		print("Registro N?: "+str(count))
		cargando_bigquery()
	except:
		print("Vacio")


def cargando_bigquery():
	print("Cargando a BigQuery")
	system("cat itemsP.json | jq -c '.[]' > Product_offers.json")
	filename = '/home/bred_valenzuela/full_vtex/vtex/search_api/OFFERS/Product_offers.json'
	dataset_id = 'landing_zone'
	table_id = 'vtex_shopstar_search_product_offers'
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

def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/search_api/OFFERS/id_producto.json','r')
	data_from_string = f_01.read()
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		count +=1
		search_product_offers(i,count)

operacion_fenix(count)

'''
QUERY = (
    'SELECT Id,ProductId FROM `shopstar-datalake.landing_zone.shopstar_vtex_sku`')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
	temp = [row.Id,row.ProductId]
	productList.append(temp)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/search_api/OFFERS/SKU_and_product_id.json", "w")
text_file.write(string)
text_file.close()
'''