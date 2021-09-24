import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

data_from = 1
data_to = 50

def get_productIFD(id,data_from,data_to):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds"
    querystring = {"categoryId":""+str(id)+"","_from":""+str(data_from)+"","_to":""+str(data_to)+""}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
        "X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"
        }
    response = requests.request("GET", url, headers=headers, params=querystring)
    formatoJson = json.loads(response.text)
    data = formatoJson["data"]
    if data:
		data_from = data_from + 50
		data_to = data_to +50
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/lista.json", "w")
		text_file.write(string)
		text_file.close()
		get_productIFD(id,data_from,data_to)
    else:
      print("Finalizado")

QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_detail_category` ')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    get_productIFD(str(row.id),data_from,data_to)
  