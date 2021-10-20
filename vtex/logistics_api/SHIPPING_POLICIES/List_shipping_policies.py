import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict


def get_user():
	#try:
		url = "https://mercury.vtexcommercestable.com.br/api/logistics/pvt/shipping-policies"
		querystring = {"page":"4","perPage":"1","total":"39","pages":"39"}
		headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
		response = requests.request("GET", url, headers=headers, params=querystring)
		FJson = json.loads(response.text)
		result = json.dumps(FJson["items"])
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/logistics_api/SHIPPING_POLICIES/temp.json", "w")
		text_file.write(result)
		text_file.close()
		cargando_bigquery()
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
	#except:
	#	print("Error")


get_user()