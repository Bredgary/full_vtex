import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

def get_product(id):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds"
    querystring = {"categoryId":+id+,"_from":"1","_to":"10"}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
        "X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    data = response.text.encode('utf8')
    return data


formatoJson = []
listIdCategory = []
client = bigquery.Client()
cantidad = 0

# Perform a query.
QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_category` ')


query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

system("touch /home/bred_valenzuela/full_vtex/vtex/catalog_api/idsProducts.json")
for row in rows:
    listIdCategory.append(row.id)
    cantidad = cantidad+1
    print("Cant IDS Product: "+str(cantidad))

string =  json.dumps(listIdCategory)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/idsProducts.json", "w")
text_file.write(string)
text_file.close()

idsCategory=open("idsProducts.json","r")
idsCategory.read()
system("rm idsProducts.json")

for i in range(1):
    for x in idsCategory:
        ids = get_product(str(x)):
        break

print(ids)