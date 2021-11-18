import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone

class init:
    productList = []
    df = pd.DataFrame()
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def get_product(id,reg):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/"+str(id)+"/specification"
    response = requests.request("GET", url, headers=init.headers)
    Fjson = json.loads(response.text)
    if Fjson is not "[]":
        for x in Fjson:
            df1 = pd.DataFrame({
                'id': x["Id"],
                'name': x["Name"],
                'value': x["Value"]}, index=[0])
            init.df = init.df.append(df1)
            print("Registro: "+str(reg))

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_product_ID`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 1
    for row in rows:
        get_product(row.id,registro)
        registro += 1
        if registro >= 100:
            break
    

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def delete_duplicate():
    client = bigquery.Client()
    QUERY = (
        'CREATE OR REPLACE TABLE `shopstar-datalake.landing_zone.shopstar_vtex_product_specification_test` AS SELECT DISTINCT * FROM `shopstar-datalake.landing_zone.shopstar_vtex_product_specification_test`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    print(rows)

def run():
    get_params()
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    table_schema = [
        {
            "name": "id",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "name",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "value",
            "type": "INTEGER",
            "mode": "NULLABLE"
        }]

    project_id = '999847639598'
    dataset_id = 'landing_zone'
    table_id = 'shopstar_vtex_product_specification_test'

    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = format_schema(table_schema)
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    
    delete_duplicate()
    
run()