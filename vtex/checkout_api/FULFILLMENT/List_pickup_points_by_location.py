import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict


geoCoordinates="-76.99398040771484"
geoCoordinates1="-11.9689302444458"
condigoPostal="150132"

def address_by_postal(geoCoordinates,geoCoordinates1,condigoPostal):
	try:
		url = "https://mercury.vtexcommercestable.com.br/api/checkout/pub/pickup-points"
		querystring = {"geoCoordinates":""+str(geoCoordinates)+";"+str(geoCoordinates1)+"","postalCode":""+str(condigoPostal)+"","countryCode":"PER"}
		headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
		response = requests.request("GET", url, headers=headers, params=querystring)
		FJson = json.loads(response.text)
		temp = FJson["items"]
		result = json.dumps(temp)
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/checkout_api/FULFILLMENT/list_pickup.json", "w")
		text_file.write(result)
		text_file.close()
		cargando_bigquery()
	except:
		print("Vacio")

def cargando_bigquery():
	print("Cargando a BigQuery")
	#system("cat SHOPPING_CART.json | jq -c '.[]' > SHOPPING_CART_TABLE.json")
	client = bigquery.Client()
	filename = '/home/bred_valenzuela/full_vtex/vtex/checkout_api/FULFILLMENT/list_pickup.json'
	dataset_id = 'landing_zone'
	table_id = 'shopstar_vtex_list_pickup_points_by_location'
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

for x in range(1):
	address_by_postal(geoCoordinates,geoCoordinates1,condigoPostal)