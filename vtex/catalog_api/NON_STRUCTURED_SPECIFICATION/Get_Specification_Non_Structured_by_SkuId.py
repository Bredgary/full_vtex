import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
listaIDS = []
listIdSkuAndContext =[]

DIR = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/NON_STRUCTURED_SPECIFICATION/NON_STRUCTURED_SPECIFICATION/'
delimitador = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])
count = 0


def get_specification_non_structured(id,count,delimitador):
	jsonF = {}
	if count >= delimitador:
		try:
			url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/specification/nonstructured/"+str(id)+""
			headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
			response = requests.request("GET", url, headers=headers)
			jsonF = json.loads(response.text)
			string = json.dumps(jsonF)
			text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/NON_STRUCTURED_SPECIFICATION/NON_STRUCTURED_SPECIFICATION/"+str(count)+"_sku.json", "w")
			text_file.write(string)
			text_file.close()
			print("Get_Specification_Non_Structured_by_SkuId.py Terminando: "+str(count))
		except:
			delimitador = count
			text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/NON_STRUCTURED_SPECIFICATION/delimitador.txt", "w")
			text_file.write(str(delimitador))
			text_file.close()
			system("python3 Get_Specification_Non_Structured_by_SkuId.py")


def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/NON_STRUCTURED_SPECIFICATION/id_sku.json','r')
	data_from_string = f_01.read()
	data_from_string = data_from_string.replace('"', '')
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		count += 1
		get_specification_non_structured(i,count,delimitador)
	print(str(count)+" registro almacenado.")

operacion_fenix(count)

'''

DIR = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/SKU/'
countDir = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])

for x in range(countDir):
    uri = "/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/SKU/"+str(x)+"_get_SKU.json"
    if os.path.exists(uri):
        f_03 = open (uri,'r')
        ids_string = f_03.read()
        formatoJson = json.loads(ids_string)
        listaID.append(formatoJson)
        print("Producto Almacenados: " +str(count))
    else:
        print("Json no existe")

string = json.dumps(listaID)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/sku.json", "w")
text_file.write(string)
text_file.close() 

system("cat sku.json | jq -c '.[]' > tableSku.json")

print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/tableSku.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_sku'
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
system("rm sku.json")
print("finalizado")
'''
