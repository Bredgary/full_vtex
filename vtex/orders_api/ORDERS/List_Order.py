import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

day = datetime.today().strftime('%d')
mouth = datetime.today().strftime('%m')
year = datetime.today().strftime('%y')
dayFrom = int(day) - 26
dayTo = int(day) - 27
limite = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30]
formatoJson = {}
formatoList = []
listDetails = []
list_order = []

def get_order(ids):
    url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(ids)+""
    headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    response = requests.request("GET", url, headers=headers)
    formatoJ = json.loads(response.text)
    return formatoJ

def get_list(pag):
    url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/?page="+str(pag)+""
    querystring = {"f_creationDate":"creationDate:[20"+str(year)+"-"+str(mouth)+"-"+str(dayFrom)+"T02:00:00.000Z TO 20"+str(year)+"-"+str(mouth)+"-"+str(dayTo)+"T01:59:59.999Z]","f_hasInputInvoice":"false"}
    headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    formatoJson = json.loads(response.text)
    return formatoJson

def load_big_query(lista,tableName):
    string = json.dumps(lista)
    text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/order_list.json", "w")
    text_file.write(string)
    text_file.close()
    system("cat order_list.json | jq -c '.[]' > tabla.json")

    print("Cargando a BigQuery "+tableName+"")
    client = bigquery.Client()
    filename = '/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/tabla.json'
    dataset_id = 'landing_zone'
    table_id = 'shopstar_vtex_'+tableName+''
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
    system("/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/rm tabla.json")
    system("/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/rm order_list.json")

def orderDetails_and_list():
    for i in limite:
        x = get_list(i)
        if bool(x["list"]):
            lista = x["list"]
            formatoList.append(lista)
            for s in x["list"]:
                details = s["orderId"]
                listDetails.append(details)
            list_order.append(x["list"])
        else:
            break

def main():
    orderDetails_and_list()
    load_big_query(listDetails,'order')
    load_big_query(list_order,'list_order')

    
main()
