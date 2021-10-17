import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict


fromD = 1
toD = 2
page = 1


def get_order_list(fromD,toD,page):
	for x in range(30):
		try:
			url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders?page="+str(page)+""
			querystring = {"f_creationDate":"creationDate:[2020-03-"+str(fromD)+"T02:00:00.000Z TO 2020-03-"+str(toD)+"T01:59:59.999Z]","f_hasInputInvoice":"false"}
			headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
			response = requests.request("GET", url, headers=headers, params=querystring)
			FJson = json.loads(response.text)
			res = FJson['list']
			result = json.dumps(res)
			text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/list.json", "w")
			text_file.write(result)
			text_file.close() 
			print("Pagina: "+str(page))
			print("Desde: "+str(fromD)+" Hasta: "+str(toD))
			cargando_bigquery()
			page +=1
		except:
			print("Lista Vacia")
			print("From: "+str(fromD)+" To:"+str(toD))

def cargando_bigquery():
	print("Cargando a BigQuery")
	system("cat list.json | jq -c '.[]' > table_list.json")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/table_list.json'
	dataset_id = 'landing_zone'
	table_id = 'vtex_shopstar_list_order_history'
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

for x in range(31):
	get_order_list(fromD,toD,page)
	fromD += 1
	toD += 1


