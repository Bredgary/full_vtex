import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone

class init:
    df = pd.DataFrame()
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    

def get_subCollection(id,reg):
    try:
	    url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/collection/"+str(id)+"/subcollection"
	    response = requests.request("GET", url, headers=init.headers)
	    Fjson = json.loads(response.text)
	    for x in Fjson:
		    df1 = pd.DataFrame({
		        'Id' : x["Id"],
				'CollectionId' : x["CollectionId"],
				'Name' : x["Name"],
				'Type' : x["Type"],
				'PreSale' : x["PreSale"],
				'Release': x["Release"]}, index=[0])
		    init.df = init.df.append(df1)
		    print("Registro: "+str(reg))
    except:
        print("Vacio")


def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT id FROM `shopstar-datalake.staging_zone.shopstar_vtex_collection`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 1
    for row in rows:
        get_subCollection(row.id,registro)
        registro += 1
    
def delete_duplicate():
    try:
        print("Borrando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_sub_collection` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_sub_collection`')
        query_job = client.query(QUERY)  
        rows = query_job.result()
        print(rows)
    except:
        print("Query no ejecutada")

def run():
    try:
        get_params()
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        print(df)
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_sub_collection'
    
        client  = bigquery.Client(project = project_id)
        dataset  = client.dataset(dataset_id)
        table = dataset.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = "WRITE_TRUNCATE"
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.autodetect = True
        job = client.load_table_from_json(json_object, table, job_config = job_config)
        print(job.result())
        delete_duplicate()
    except:
        print("vacio")
    
run()