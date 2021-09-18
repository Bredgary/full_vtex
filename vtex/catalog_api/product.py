import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds"
headers = {"Content-Type": "application/json",    "Accept": "application/json",    "X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",    "X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
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

for i in range[2]
    for x in idsCategory:
        ids = str(x)
        querystring = {"categoryId":"1","_from":"1","_to":"10"}
        response = requests.request("GET", url, headers=headers, params=querystring)
        formatoJson.append(response.text)

    
print(formatoJson)