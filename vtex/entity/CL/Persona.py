#!/usr/bin/python
# -*- coding: latin-1 -*-
import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
import requests

def cl_client():
	url = "https://mercury.vtexcommercestable.com.br/api/dataentities/CL/search"
	querystring = {"_fields":"email,firstName,document"}
	headers = {
		"Content-Type": "application/json",
		"Accept": "application/vnd.vtex.ds.v10+json",
		"REST-Range": "resources=0-1",
		"X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
		"X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"
	}
	response = requests.request("GET", url, headers=headers, params=querystring)
	Fjson = json.loads(response.text)
	return Fjson


def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

### Create dummy data to load
df = pd.DataFrame(cl_client(),
columns=['email', 'firstName', 'document'])

'''
### Convert dataframe to JSON object
json_data = df.to_json(orient = 'records')
json_object = json.loads(json_data)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"credentials.json"

### Define schema as on BigQuery table, i.e. the fields id, first_name and last_name   
table_schema = {
          'name': 'id',
          'type': 'INTEGER',
          'mode': 'REQUIRED'
          }, {
          'name': 'first_name',
          'type': 'STRING',
          'mode': 'NULLABLE'
          }, {
          'name': 'last_name',
          'type': 'STRING',
          'mode': 'NULLABLE'
          }

project_id = '<my_project>'
dataset_id = '<my_dataset>'
table_id = '<my_table>'

client  = bigquery.Client(project = project_id)
dataset  = client.dataset(dataset_id)
table = dataset.table(table_id)

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.schema = format_schema(table_schema)
job = client.load_table_from_json(json_object, table, job_config = job_config)

print(job.result())
'''