import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

listaID = []
formatoJson = {}
listaProductID = []
formatoJson = {}
formJson = {}
count = 0

f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT_SPECIFICATION/delimitador.txt','r')
data_from_string = f_01.read()

delimitador = int(data_from_string)

def get_RefId(id,count):
    if count >= delimitador:
        print("Comenzando: "+str(delimitador))
        try:
            print("Comenzando: "+str(count))
            url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/productgetbyrefid/"""+str(id)+""
            headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
            response = requests.request("GET", url, headers=headers)
            jsonF = json.loads(response.text)
            string = json.dumps(jsonF)
            text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/temp4/"+str(count)+"_get_RefId.json", "w")
            text_file.write(string)
            text_file.close()
            print("Terminando: "+str(count)) 

f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/listaIdRef.json','r')
data_from_string = f_01.read()

formatoJSon = json.loads(data_from_string)

for i in formatoJSon:
    count +=1
    get_RefId(i,count)
    

print(str(count)+" registro almacenado "+str(i))
print("Finalizado")


DIR = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/temp4/'
countDir = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])

for x in range(countDir):
    count +=1
    uri = "/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/temp4/"+str(count)+"_get_RefId.json"
    f_03 = open (uri,'r')
    ids_string = f_03.read()
    formatoJson = json.loads(ids_string)
    listaID.append(formatoJson)
    print("Producto Almacenados: " +str(count))

string = json.dumps(listaID)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/listaRefId.json", "w")
text_file.write(string)
text_file.close() 


system("cat listaRefId.json | jq -c '.[]' > table_listaRefId.json")


print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/table_listaRefId.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_refid_product_v2'
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
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
'''
