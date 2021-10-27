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

'''
def all_coupon():
	url = "https://mercury.vtexcommercestable.com.br/api/rnb/pvt/coupon"
	headers = {"Accept": "application/json; charset=utf-8","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers)
	FJson = json.loads(response.text)
	result = json.dumps(FJson)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/promotions_and_taxes_api/COUPONS/items.json", "w")
	text_file.write(result)
	text_file.close()
	cargando_bigquery()

def cargando_bigquery():
	print("Cargando a BigQuery")
	system("cat items.json | jq -c '.[]' > all_coupon.json")
	filename = '/home/bred_valenzuela/full_vtex/vtex/promotions_and_taxes_api/COUPONS/all_coupon.json'
	dataset_id = 'landing_zone'
	table_id = 'vtex_shopstar_all_coupon'
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

all_coupon()
'''

QUERY = (
    'SELECT couponCode FROM `shopstar-datalake.landing_zone.vtex_shopstar_all_coupon`')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
    productList.append(row.couponCode)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/promotions_and_taxes_api/COUPONS/COUPONS_ID.json", "w")
text_file.write(string)
text_file.close()