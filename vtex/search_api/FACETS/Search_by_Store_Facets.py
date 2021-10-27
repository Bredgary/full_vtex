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
order = {}


def search_by_store_facets(id,count):
	#try:
	url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pub/facets/search/"+str(id)+""
	querystring = {"map":"c"}
	headers = {"Accept": "application/json; charset=utf-8","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	Summary = FJson["Summary"]
	while("" in FJson) :
		FJson.remove("")
	while(None in Summary) :
		Summary.remove("")
	print(Summary)
	#result = json.dumps(FJson)
	#text_file = open("/home/bred_valenzuela/full_vtex/vtex/search_api/FACETS/items.json", "w")
	#text_file.write(result)
	#text_file.close()
	#print("Registro N°: "+str(count))
	#cargando_bigquery()
	#except:
	#	url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pub/facets/search/"+str(id)+""
	#	querystring = {"map":"c"}
	#	headers = {"Accept": "application/json; charset=utf-8","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	#	response = requests.request("GET", url, headers=headers, params=querystring)
	#	if response:
	#		print("Tabla Vacia")
	#	else:
	#		text_file = open("/home/bred_valenzuela/full_vtex/vtex/search_api/FACETS/respaldo/itemsS.json", "w")
	#		text_file.write(response.text)
	#		text_file.close()


def cargando_bigquery():
	print("Cargando a BigQuery")
	#system("cat items.json | jq -c '.[]' > category_facets.json")
	filename = '/home/bred_valenzuela/full_vtex/vtex/search_api/FACETS/items.json'
	dataset_id = 'landing_zone'
	table_id = 'vtex_shopstar_search_by_store_facets'
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
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/search_api/FACETS/id_category.json','r')
	data_from_string = f_01.read()
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		count +=1
		search_by_store_facets(i,count)
		break

operacion_fenix(count)

'''
QUERY = (
    'SELECT id FROM `shopstar-datalake.Shopstar_test.shopstar_vtex_category`')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
	productList.append(row.id)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/search_api/FACETS/id_category.json", "w")
text_file.write(string)
text_file.close()
'''