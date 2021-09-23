#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os, sys
import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
columns = ""
productList = []
registro = 0
temp2 = "" 
list1 = ""
headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def replace_blank_dict(d):
    if not d:
        return None
    if type(d) is list:
        for list_item in d:
            if type(list_item) is dict:
                for k, v in list_item.items():
                    list_item[k] = replace_blank_dict(v)
    if type(d) is dict:
        for k, v in d.items():
            d[k] = replace_blank_dict(v)
    return d

def get_sku_list(id,headers):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitByProductId/"""+(str(id))+""
    response = requests.request("GET", url, headers=headers) 
    #print(list(map(int, response.content)))
    jsonF = json.loads(response.text)
    return jsonF

QUERY = (
    'SELECT Id FROM `shopstar-datalake.landing_zone.shopstar_vtex_product` ')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    temp = get_sku_list(str(row.Id))
    productList.append(temp)

for order in productList:
    for k, v in order.items():
        order[k] = replace_blank_dict(v)


print("Comenzando la conversión")
columns = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/context.json", "w")
text_file.write(columns)
text_file.close() 
system("cat context.json | jq -c '.[]' > table.json")

print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/table.json'
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
system("rm context.json")
system("rm table.json")
print("finalizado")
