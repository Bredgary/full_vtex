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
    #try:
        url = "https://mercury.vtexpayments.com.br/api/pvt/affiliations"
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        for x in Fjson:
        	df1 = pd.DataFrame({
				'id' : x["id"],
				'implementation' : x["implementation"],
				'name' : Fjson["name"],
				'isdelivered' : x["isdelivered"],
				'isConfigured': x["isConfigured"]}, index=[0])
        	init.dfAffi = init.df.append(df1)
    #except:
     #   print("Vacio")

def get_config():
    #try:
    	url = "https://mercury.vtexpayments.com.br/api/pvt/affiliations"
    	response = requests.request("GET", url, headers=init.headers)
    	Fjson = json.loads(response.text)
    	config = Fjson["configuration"]
    	for x in Fjson:
    		for y in config:
    			df1 = pd.DataFrame({
					'affiliationsId' : x["id"],
					'name' : y["name"],
					'value' : y["value"],
					'valueKey': y["valueKey"]}, index=[0])
    			init.dfCon = init.df.append(df1)
    #except:
    #    print("Vacio")

    
def delete_duplicate():
    try:
        print("Borrando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_affiliations` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_affiliations`')
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
            'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_affiliations_conf` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_affiliations_conf`')
        query_job = client.query(QUERY)  
        rows = query_job.result()
        print(rows)
    except:
        print("Query no ejecutada")

def run():
	#try:
		dfAffi = init.dfAffi
		dfConf = init.dfCon
		
		dfAffi.reset_index(drop=True, inplace=True)
		json_data_dfAffi = dfAffi.to_json(orient = 'records')
		json_object_dfAffi = json.loads(json_data_dfAffi)
		
		dfConf.reset_index(drop=True, inplace=True)
		json_data_dfConf = dfConf.to_json(orient = 'records')
		json_object_dfConf = json.loads(json_data_dfConf)
		
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
		job_config.autodetect = True
		job = client.load_table_from_json(json_object_dfAffi, tableO, job_config = job_config)
		print(job.result())
		
		tableT = dataset.table(table_temp)
		job_config_temp = bigquery.LoadJobConfig()
		job_config_temp.write_disposition = "WRITE_TRUNCATE"
		job_config_temp.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
		job_config.autodetect = True
		job = client.load_table_from_json(json_object_dfConf, tableT, job_config = job_config_temp)
		print(job.result())
		delete_duplicate()
		delete_duplicate_2()
	#except:
	#	print("vacio")

run()