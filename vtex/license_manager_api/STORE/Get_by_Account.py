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
    id = None
    name = None
    hosts = None
        
def get_account():
    try:
    	url = "https://mercury.vtexcommercestable.com.br/api/vlm/account/stores"
    	response = requests.request("GET", url, headers=headers)
    	Fjson = json.loads(response.text)
    	for x in Fjson:
    		init.id = x["id"]
    		init.name = x["name"]
    		hosts = x["hosts"]
    		for i in hosts:
    			init.hosts = i
    	df1 = pd.DataFrame({
			'id' : init.id,
			'name' : init.name,
			'hosts' : init.hosts}, index=[0])
    	init.df = init.df.append(df1)
    	print("Registro: "+str(reg))
    except:
    	print("Vacio")

    
def delete_duplicate():
    try:
        print("Borrando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_account` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_account`')
        query_job = client.query(QUERY)  
        rows = query_job.result()
        print(rows)
    except:
        print("Query no ejecutada")

def run():
    #try:
        get_account()
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_account'
    
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
    #except:
    #    print("vacio")
    
run()