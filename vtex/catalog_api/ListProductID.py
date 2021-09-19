import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery


client = bigquery.Client()
listIdCategory = []
productF = []
productList = []

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
    productF.append(jsonF)
    for order in OrderF:
        for k, v in order.items():
            order[k] = replace_blank_dict(v)
    return productF

def get_productIFD(id):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds"
    querystring = {"categoryId":""+str(id)+"","_from":"0","_to":"50"}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
        "X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"
        }
    response = requests.request("GET", url, headers=headers, params=querystring)
    jsonF = json.loads(response.text)  
    productList.append(jsonF)
    return productList

QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_category` ')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    listIdCategory.append(row.id)

for i in listIdCategory:
    get_productIFD(i)


tableProduct =  json.dumps(productF)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/temp.json", "w")
text_file.write(tableProduct)
text_file.close() 
system("cat temp.json | jq -c '.[]' > Product.json")


client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/Product.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_product'
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
system("rm DetailOrdersFinal.json")
system("rm temp.json")
print("finalizado")

print("Finalizado")