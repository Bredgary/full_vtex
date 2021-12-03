#!/usr/bin/python
# -*- coding: latin-1 -*-
import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join

class init:
    productList = []
    df = pd.DataFrame()
    postalCode = None
    city = None
    state = None
    country = None
    street = None
    number = None
    neighborhood = None
    complement = None
    reference = None
    geoCoordinates = None
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}


     
def get_code_postal(countryCode,postalCode,reg):
    url = "https://mercury.vtexcommercestable.com.br/api/checkout/pub/postal-code/"+str(countryCode)+"/"+str(postalCode)+""
    response = requests.request("GET", url, headers=init.headers)
    Fjson = json.loads(response.text)
    print(Fjson["postalCode"])
    print(Fjson["city"])
    print(Fjson["state"])
    print(Fjson["country"])
    print(Fjson["street"])
    print(Fjson["number"])
    print(Fjson["neighborhood"])
    print(Fjson["complement"])
    print(Fjson["reference"])
    geo = Fjson["geoCoordinates"]
    print(geo[0])
    print(geo[1])
    
    #df1 = pd.DataFrame({
    #    'orderId': init.orderId,
    #    'emailTracked': emailTr,
    #    'invoicedDate': init.invoicedDate}, index=[0])
    #init.df = init.df.append(df1)
    #print("Registro: "+str(reg))
        

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT storePreferencesData_countryCode,postalCode FROM `shopstar-datalake.staging_zone.shopstar_vtex_order` where storePreferencesData_countryCode is not null and postalCode is not null')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 1
    for row in rows:
        get_code_postal(row.storePreferencesData_countryCode,row.postalCode,registro)
        registro += 1

def delete_duplicate():
	try:
		print("Eliminando duplicados")
		client = bigquery.Client()
		QUERY = (
			'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_address_by_postal_code` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_address_by_postal_code`')
		query_job = client.query(QUERY)
		rows = query_job.result()
		print(rows)
	except:
		print("Consulta SQL no ejecutada")



def run():
  #  try:
        get_params()
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        '''
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_address_by_postal_code'
        
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
        '''
 #   except:
  #      print("Error")
    
run()