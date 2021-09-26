import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

dia = datetime.today().strftime('%d')
dia1 = int(dia) - 25
dia2 = int(dia) - 24

limite = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30]
temporal = {}
list_orders=[]
dict = defaultdict(list)
formatoJson = {}
formatoList = []
count = 0

def get_list(pag):
	url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/?page="+str(pag)+""
	querystring = {"f_creationDate":"creationDate:[2021-09-"+str(dia1)+"T02:00:00.000Z TO 2021-09-"+str(dia2)+"T01:59:59.999Z]","f_hasInputInvoice":"false"}
	headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	formatoJson = json.loads(response.text)
	return formatoJson

for i in limite:
    x = get_list(i)
    if bool(x["list"]):
        lista = x["list"]
        string = str(lista)[1:-1]
        formatoList.append(string)
    else:
        break


string = json.dumps(formatoList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/order_list.json", "w")
text_file.write(string)
text_file.close()

system("cat order_list.json | jq -c '.[]' > tabla.json")

'''
print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/tabla.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_list_orders_v2'
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