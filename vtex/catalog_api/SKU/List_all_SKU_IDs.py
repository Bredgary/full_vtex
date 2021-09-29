import requests
import json
import os, os.path
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

limite = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30]
formatoJson = {}
formatoList = []
listDetails = []
list_sku = []

def get_sku(ids):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"""+str(id)+""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
        "X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"
        }
    response = requests.request("GET", url, headers=headers)
    formatoJ = json.loads(response.text)
    return formatoJ
    
def get_list(page):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitids?page="+str(pag)+"&pagesize="+str(pagSize)+""
    querystring = {"page":"+str(page)+","pagesize":"+str(pageSize)+"}
    headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    formatoJson = json.loads(response.text)
    return formatoJson
    
 for i in limite:
    x = get_list(i)
    if bool(x["list"]):
        lista = x["list"]
        formatoList.append(lista)
        for s in x["list"]:
            details = get_sku(s["skuId"])
            listDetails.append(details)
        list_sku.append(x["list"])
    else:
        break

#Detalle
string = json.dumps(listDetails)
characters = "@"
string = ''.join( x for x in string if x not in characters)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/order.json", "w")
text_file.write(string)
text_file.close()
system("cat sku.json | jq -c '.[]' > tabla_sku.json")


print("Cargando a BigQuery order")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/tabla_sku.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_sku_k'
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
system("rm sku.json")
system("rm tabla_sku.json")


#Lista
string = json.dumps(list_sku)
characters = "@"
string = ''.join( x for x in string if x not in characters)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/list_sku.json", "w")
text_file.write(string)
text_file.close()
system("cat list_sku.json | jq -c '.[]' > temp.json")
system("cat temp.json | jq -c '.[]' > tabla_list_sku.json")


print("Cargando a BigQuery list sku")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/tabla_list_sku.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_sku_k'
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
system("rm list_sku.json")
system("rm temp.json")
system("rm tabla_list_sku.json")