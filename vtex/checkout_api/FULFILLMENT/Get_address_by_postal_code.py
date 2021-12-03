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
import shapely.geometry
import shapely.wkt

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
    latitude = None
    longitude = None
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}


     
def get_code_postal(countryCode,postalCode,reg):
    url = "https://mercury.vtexcommercestable.com.br/api/checkout/pub/postal-code/"+str(countryCode)+"/"+str(postalCode)+""
    response = requests.request("GET", url, headers=init.headers)
    Fjson = json.loads(response.text)
    
    init.postalCode = Fjson["postalCode"]
    init.city = Fjson["city"]
    init.state = Fjson["state"]
    init.country = Fjson["country"]
    init.street = Fjson["street"]
    init.number = Fjson["number"]
    init.neighborhood = Fjson["neighborhood"]
    init.complement = Fjson["complement"]
    init.reference = Fjson["reference"]
    init.geoCoordinates = Fjson["geoCoordinates"]
    init.latitude = init.geoCoordinates[0]
    init.longitude = init.geoCoordinates[1]
    
    
    print(geopoint)
    print(type(geopoint))
    geography = shapely.geometry.LineString([(init.latitude , 33.9416), (init.longitude , 40.6413)])
    print(geography)
    print(type(geography))

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