#!/usr/bin/python
# -*- coding: latin-1 -*-
import os, sys
import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()

formatoJson = {}
productList = [] 
temp = []
registro = 0
temp2 = "" 
list1 = ""
headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
registro = []


def get_sku_list(id,headers):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitByProductId/"""+(str(id))+""
    response = requests.request("GET", url, headers=headers) 
    #formatoJson = json.loads(response.text)
    temp.append(response.text)
    

QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_product` ')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    get_sku_list(str(row),headers)
    break
#    registro +=1
#    print("Registros almacenados en archivo temporal: "+ str(registro))
#    
    

#print("Comenzando la conversión")

#def listToStringWithoutBrackets(list1):
#    return str(list1).replace('[','').replace(']','') 

#temp2 = listToStringWithoutBrackets(temp)

#print(temp2)
'''
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/context.json", "w")
text_file.write(temp2)
text_file.close() 

system("cat context.json | jq -c '.[]' > newNDJSON.json")
print("prueba terminada")

print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/newNDJSON.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_sku_list_by_productid'
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.autodetect = True
with open(filename, "rb") as source_file: 
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location="southamerica-east1",  # Must match the destination dataset location.
    job_config=job_config,)  # API request
job.result()  # Waits for table load to complete.
print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
print("finalizado")
'''