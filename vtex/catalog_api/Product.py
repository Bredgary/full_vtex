import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

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
	url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/product/441"
	headers = {
    	"Content-Type": "application/json",
    	"Accept": "application/json",
    	"X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
    	"X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"
	}
	response = requests.request("GET", url, headers=headers)
    data = response.text.encode('utf8')
    #formatoJson = json.loads(response.text)
    #dataProduct = formatoJson["data"]
    #for i in dataProduct:
    #  productList.append(i)
    return data

idsProduct=open("lista","r")
idsProduct.read()
system("rm lista.json")

for i in idsProduct:
	print("ID Producto"+str(i)







