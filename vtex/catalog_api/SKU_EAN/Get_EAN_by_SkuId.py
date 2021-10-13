import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
listaID = []
listIdSkuAndContext =[]
registro = 0

DIR = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/SKU_EAN'
delimitador = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])
count = 0

def get_ean(id,count,delimitador):
	jsonF = {}
	if count >= delimitador:
		try:
			url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"+str(id)+"/ean"
			headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
			response = requests.request("GET", url, headers=headers)
			print(type(response.text))
			text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/SKU_EAN/"+str(count)+"_sku_ean.json", "w")
			text_file.write(response.text)
			text_file.close()
			print("Get_EAN_by_SkuId.py Terminando: "+str(count))
		except:
			delimitador = count
			text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/delimitador.txt", "w")
			text_file.write(str(delimitador))
			text_file.close()
			system("python3 Get_EAN_by_SkuId.py")


def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/id_sku.json','r')
	data_from_string = f_01.read()
	data_from_string = data_from_string.replace('"', '')
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		count += 1
		get_ean(i,count,delimitador)
		break
	print(str(count)+" registro almacenado.")

operacion_fenix(count)

'''
DIR = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/SKU_EAN/'
countDir = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])

for x in range(countDir):
	registro = registro + 1
	uri = "/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/SKU_EAN/"+str(registro)+"_sku_ean.json"
	f_03 = open (uri,'r')
	ids_string = f_03.read()
	if ids_string != '""':
		print(int(ids_string))
		listaID.append(ids_string)
		#print("Almacenados: " +str(registro))


text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/temp.json", "w")
text_file.write(str(ids_string))
text_file.close() 

system("cat temp.json | jq -c '.[]' > tableSkuEan.json")

print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/tableSkuEan.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_sku_ean'
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
