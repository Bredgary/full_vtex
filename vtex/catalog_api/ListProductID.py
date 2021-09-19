import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

CFrom = 0
CTo = 0
OrderF = []
contador = 0
total = 0

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
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds"
    querystring = {"categoryId":""+str(id)+"","_from":"0","_to":"50"}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
        "X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    data = response.text.encode('utf8')
    formatoJson = json.loads(response.text)
    range = formatoJson["range"]
    total = range["total"]
    #print("El ID: "+str(id)+" tiene "+str(total)+" productos")
    return data

def total(id):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds"
    querystring = {"categoryId":""+str(id)+"","_from":"1","_to":"10"}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
        "X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    data = response.text.encode('utf8')
    formatoJson = json.loads(response.text)
    range = formatoJson["range"]
    total = range["total"]
    return total


formJson = []
listIdCategory = []
client = bigquery.Client()
cantidad = 0
temp = 0

# Perform a query.
QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_category` ')


query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

system("touch /home/bred_valenzuela/full_vtex/vtex/catalog_api/idsProducts.json")
for row in rows:
    listIdCategory.append(row.id)

string =  json.dumps(listIdCategory)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/idsProducts.json", "w")
text_file.write(string)
text_file.close()
idsCategory=open("idsProducts.json","r")
idsCategory.read()
system("rm idsProducts.json")

for i in listIdCategory:
    print("Category: "+str(i))
    orderDe = get_product(str(i))
    OrderF.append(orderDe)

#fjson =  json.loans(OrderF)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/lista.json", "w")
text_file.write(orderDe)
text_file.close() 
#system("cat temp.json | jq -c '.[]' > IdProducts.json")
print("Finalizado")