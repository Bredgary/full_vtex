import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join
from _queue import Empty

class init:
  productList = []
  df = pd.DataFrame()
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def format_schema(schema):
  formatted_schema = []
  for row in schema:
    formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def decrypt_email(email):
  try:
    url = "https://conversationtracker.vtex.com.br/api/pvt/emailMapping?an=mercury&alias="+email+""
    response = requests.request("GET", url, headers=init.headers)
    formatoJ = json.loads(response.text)
    return formatoJ["email"]
  except:
    print(str(email))

def get_order(id):
    url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
    response = requests.request("GET", url, headers=init.headers)
    Fjson = json.loads(response.text)
    print(Fjson["invoiceData"])

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT orderId  FROM `shopstar-datalake.test.shopstar_ft_orders_`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId)
        print("Registro: "+str(registro))
        
get_params()