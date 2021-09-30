import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
listaIDS = []

f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT_SPECIFICATION/delimitador.txt','r')
data_from_string = f_01.read()
delimitador = int(data_from_string)
count = 0

def get_specification(id,count,delimitador):
    if count >= delimitador:
        print("Comenzando: "+str(count))
        try:
            url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/"""+str(id)+"""/specification"""
            headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
            response = requests.request("GET", url, headers=headers)
            jsonF = json.loads(response.text)
            listaIDS.append(jsonF)
            print(str(count)+" registro almacenado")
        except:
            delimitador = count
            text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT_SPECIFICATION/delimitador.txt", "w")
            text_file.write(str(delimitador))
            text_file.close()
            system("python3 Get_Product_Specificationby_ProductID.py")
        

QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_product_v2`')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    count += 1
    get_specification(str(row.id),count,delimitador)


string = json.dumps(listaIDS)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT_SPECIFICATION/lista.json", "w")
text_file.write(string)
text_file.close() 

'''
system("cat lista.json | jq -c '.[]' > context.json")

print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/context.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_specification_product'
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
system("rm context.json")
system("rm lista.json")
print("finalizado")
'''