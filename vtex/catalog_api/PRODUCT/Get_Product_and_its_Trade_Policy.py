import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
productList = []
listIdProductAndContext = []
listaIDS = []
count = 0

def get_policy(id,count):
    if count >= 40122:
        print("Comenzando: "+str(count))
        url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/productget/"""+str(id)+""
        headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
        response = requests.request("GET", url, headers=headers)
        jsonF = json.loads(response.text)
        string = json.dumps(jsonF)
        text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/temp3/"+str(count)+"_get_policy.json", "w")
        text_file.write(string)
        text_file.close()
        print("Terminando: "+str(count)) 

f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/lista.json','r')
data_from_string = f_01.read()

formatoJSon = json.loads(data_from_string)

for i in formatoJSon:
    count +=1
    get_policy(i,count)
    

print(str(count)+" registro almacenado "+str(i))
print("Finalizado")

'''

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/lista.json", "w")
text_file.write(string)
text_file.close() 

system("cat lista.json | jq -c '.[]' > table.json")

print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/table.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_policy_product_v2'
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
system("rm table.json")
system("rm lista.json")
print("finalizado")
'''