import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

def :
	FJson = json.loads(response.text)
	result = json.dumps(FJson)
	text_file = open("items.json", "w")
	text_file.write(result)
	text_file.close()
	print("Registro: "+str())
	cargando_bigquery()

def cargando_bigquery():
	print("Cargando a BigQuery")
	system("cat items.json | jq -c '.[]' > .json")
	client = bigquery.Client()
	filename = '.json'
	dataset_id = 'landing_zone'
	table_id = ''
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

'''
QUERY = (
    '')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
    productList.append(row.FieldId)

string = json.dumps(productList)
text_file = open("_2.json", "w")
text_file.write(string)
text_file.close()
'''