#!/usr/bin/python
# -*- coding: latin-1 -*-
import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone

naive_dt = datetime.now()
aware_dt = naive_dt.astimezone()
# correct, ISO-8601 (but not UTC)
aware_dt.isoformat(timespec="seconds")
# lets get the time in UTC
utc_dt = aware_dt.astimezone(timezone.utc)
# correct, ISO-8601 and UTC (but not in UTC format)
date_str = utc_dt.isoformat(timespec='milliseconds')
date = date_str.replace("+00:00", "Z")

now = datetime.now()
format = now.strftime('%Y-%m-%d')


def cl_client():
	url = "https://mercury.vtexcommercestable.com.br/api/dataentities/CL/search"
	querystring = {"_fields":"email,firstName,document","_where":"createdIn="+format+""}
	headers = {
		"Content-Type": "application/json",
		"Accept": "application/vnd.vtex.ds.v10+json",
		"REST-Range": "resources=0-1000",
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

df = pd.DataFrame(cl_client(),
columns=['email', 'firstName', 'document'])

json_data = df.to_json(orient = 'records')
json_object = json.loads(json_data)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"/home/bred_valenzuela/full_vtex/vtex/entity/CL/key.json"

table_schema = {
	"name": "email",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "firstName",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "document",
    "type": "STRING",
    "mode": "NULLABLE"}


project_id = '999847639598'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_client'

client  = bigquery.Client(project = project_id)
dataset  = client.dataset(dataset_id)
table = dataset.table(table_id)

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.schema = format_schema(table_schema)
job = client.load_table_from_json(json_object, table, job_config = job_config)
print(job.result())
