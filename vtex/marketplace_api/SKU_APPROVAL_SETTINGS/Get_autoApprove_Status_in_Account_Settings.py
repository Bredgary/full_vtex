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



def get_autoapprove_status_in_account_settings(x,count):
	#try:
	url = "https://api.vtex.com/mercury/suggestions/configuration/autoapproval/toggle"
	querystring = {"sellerId":""+x+""}
	headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	result = json.dumps(FJson)
	seller = {"id":x}
	d4 = dict(seller.items() + result.items())
	print(type(d4))
	print(d4)
	#result2 = seller + result
	#print(result2)
	#text_file = open("/home/bred_valenzuela/full_vtex/vtex/logistics_api/SHIPPING_POLICIES/temp.json", "w")
	#text_file.write(result)
	#text_file.close()
	#cargando_bigquery()
	#except:
	#	print("Error")

def cargando_bigquery():
	#try:
	print("Cargando a BigQuery")
	system("cat temp.json | jq -c '.[]' > table_shipping_policies.json")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/logistics_api/SHIPPING_POLICIES/table_shipping_policies.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_list_shipping_policies'
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
	#except:
	#	print("Error")

def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/marketplace_api/SKU_APPROVAL_SETTINGS/id_seller.json','r')
	data_from_string = f_01.read()
	listaIDS = json.loads(data_from_string)
	for i in range(int(len(listaIDS))):
		x = listaIDS[i]
		get_autoapprove_status_in_account_settings(x,count)
		count += 1
		print(str(count)+" registro almacenado.")


operacion_fenix(count)

'''
QUERY = (
    'SELECT SellerId FROM `shopstar-datalake.landing_zone.shopstar_vtex_seller`')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
    productList.append(row.SellerId)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/marketplace_api/SKU_APPROVAL_SETTINGS/id_seller.json", "w")
text_file.write(string)
text_file.close()
'''