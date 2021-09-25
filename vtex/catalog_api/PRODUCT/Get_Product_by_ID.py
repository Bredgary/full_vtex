import requests
import json
import os, os.path
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
listaID = []
formatoJson = {}
listaProductID = []
formatoJson = {}
formJson = {}

DIR = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/'
countDir = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])
rangoDir = countDir - 6

for x in range(rangoDir):
    uri = "/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/"+str(x)+"_productID_categoryID_441.json"
    f_03 = open (uri,'r')
    ids_string = f_03.read()
    formatoJson = json.loads(ids_string)
    break

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

def get_product(id):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/product/"""+str(id)+""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
        "X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"
        }
    response = requests.request("GET", url, headers=headers)
    jsonF = json.loads(response.text)
    #for order in jsonF:
    #    for k, v in order.items():
    #        order[k] = replace_blank_dict(v)
    return jsonF

for x in formatoJson:
    producto = get_product(x)
    #formJson = json.loads(producto)
    listaID.append(producto)
    print(type(producto))

string = json.dumps(listaID)
#text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/lista.json", "w")
#text_file.write(string)
#text_file.close() 

#system("cat lista.json | jq -c '.[]' > tabla.json")

'''
print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/tabla.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_product_v2'
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
