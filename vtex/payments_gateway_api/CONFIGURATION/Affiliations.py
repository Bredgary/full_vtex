import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone

class init:
    dfAffi = pd.DataFrame()
    dfCon = pd.DataFrame()
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    

def get_aff():
    try:
        url = "https://mercury.vtexpayments.com.br/api/pvt/affiliations"
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        for x in Fjson:
        	df1 = pd.DataFrame({
				'id' : Fjson["id"],
				'implementation' : Fjson["implementation"],
				'name' : Fjson["name"],
				'isdelivered' : Fjson["isdelivered"],
				'isConfigured': Fjson["isConfigured"]}, index=[0])
        	init.dfAffi = init.df.append(df1)
    except:
        print("Vacio")

def get_config():
    try:
    	url = "https://mercury.vtexpayments.com.br/api/pvt/affiliations"
    	response = requests.request("GET", url, headers=init.headers)
    	Fjson = json.loads(response.text)
    	config = Fjson["configuration"]
    	for x in Fjson:
    		df1 = pd.DataFrame({
				'affiliationsId' : Fjson["id"],
				'name' : config["name"],
				'value' : config["value"],
				'valueKey': config["valueKey"]}, index=[0])
    		init.dfCon = init.df.append(df1)
    except:
        print("Vacio")

    
def delete_duplicate():
    try:
        print("Borrando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_specification_field` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_specification_field`')
        query_job = client.query(QUERY)  
        rows = query_job.result()
        print(rows)
    except:
        print("Query no ejecutada")
        
def delete_duplicate_2():
    try:
        print("Borrando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_specification_field` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_specification_field`')
        query_job = client.query(QUERY)  
        rows = query_job.result()
        print(rows)
    except:
        print("Query no ejecutada")

def run():
    try:
    	dfAffi = init.dfAffi
    	dfCon = init.dfCon
    	
        dfAffi.reset_index(drop=True, inplace=True)
        json_data = dfAffi.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        dfCon.reset_index(drop=True, inplace=True)
        json_data = dfCon.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_affiliations'
        table_temp = 'shopstar_vtex_affiliations_conf'
        
        client  = bigquery.Client(project = project_id)
        dataset  = client.dataset(dataset_id)
        tableO = dataset.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = "WRITE_TRUNCATE"
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = format_schema(table_schema)
        job = client.load_table_from_json(json_object, tableO, job_config = job_config)
        print(job.result())
        
        tableT = dataset.table(table_temp)
        job_config_temp = bigquery.LoadJobConfig()
        job_config_temp.write_disposition = "WRITE_TRUNCATE"
        job_config_temp.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = format_schema(table_schema)
        job = client.load_table_from_json(json_object, tableT, job_config = job_config_temp)
        print(job.result())
        delete_duplicate()
        delete_duplicate_2()
    except:
        print("vacio")

run()