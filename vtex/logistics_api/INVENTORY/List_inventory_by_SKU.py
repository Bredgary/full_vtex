import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from os.path import join
import logging

class init:
  df = pd.DataFrame()
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
  

def get_inventory(id):
	try:
		url = "https://mercury.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/"+str(id)+""
		response = requests.request("GET", url, headers=init.headers)
		Fjson = json.loads(response.text)
		balance = Fjson["balance"]
		
		for x in balance:
			warehouseId = x["warehouseId"]
			warehouseName = x["warehouseName"]
			totalQuantity = x["totalQuantity"]
			reservedQuantity = x["reservedQuantity"]
			hasUnlimitedQuantity = x["hasUnlimitedQuantity"]
			timeToRefill = x["timeToRefill"]
			dateOfSupplyUtc = x["dateOfSupplyUtc"]
			
			df1 = pd.DataFrame({
				'SKU_ID': id,
				'warehouseId': warehouseId,
				'warehouseName': warehouseName,
				'totalQuantity': totalQuantity,
				'reservedQuantity': reservedQuantity,
				'hasUnlimitedQuantity': hasUnlimitedQuantity,
				'timeToRefill': timeToRefill,
				'dateOfSupplyUtc': dateOfSupplyUtc}, index=[0])
			init.df = init.df.append(df1)
	except:
		print("Vacio")


def delete_duplicate():
  try:
    print("Eliminando duplicados")
    client = bigquery.Client()
    QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_list_inventory_by_sku` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_inventory_by_sku`')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print(rows)
  except:
    print("Consulta SQL no ejecutada")


def run():
    try:
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_list_inventory_by_sku'
        
        if df.empty:
            print('DataFrame is empty!')
        else:
            client  = bigquery.Client(project = project_id)
            dataset  = client.dataset(dataset_id)
            table = dataset.table(table_id)
            job_config = bigquery.LoadJobConfig()
            #.write_disposition = "WRITE_TRUNCATE"
            #job_config.autodetect = True
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job = client.load_table_from_json(json_object, table, job_config = job_config)
            print(job.result())
            delete_duplicate()
    except:
        print("Error.")
        logging.exception("message")

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT id  FROM `shopstar-datalake.staging_zone.shopstar_vtex_sku`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_inventory(row.id)
        print("Registro: "+str(registro))
        if registro == 10000:
            run()
        if registro == 20000:
            run()
        if registro == 30000:
            run()
        if registro == 40000:
            run()
        if registro == 50000:
            run()
        if registro == 60000:
            run()
        if registro == 70000:
            run()
        if registro == 80000:
            run()
        if registro == 90000:
            run()
        if registro == 100000:
            run()
        if registro == 110000:
            run()
        if registro == 120000:
            run()
        if registro == 130000:
            run()
        if registro == 140000:
            run()
        if registro == 150000:
            run()
        if registro == 160000:
            run()
        if registro == 170000:
            run()
        if registro == 180000:
            run()
        if registro == 190000:
            run()
        if registro == 200000:
            run()
        if registro == 210000:
            run()
        if registro == 220000:
            run()
        if registro == 230000:
            run()
        if registro == 240000:
            run()
        if registro == 250000:
            run()
        if registro == 260000:
            run()
        if registro == 270000:
            run()
        if registro == 280000:
            run()
        if registro == 290000:
            run()
        if registro == 300000:
            run()
    run()

get_params()
