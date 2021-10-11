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
registro=0
'''
DIR = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_ATTACHMENT/SKU_ATTACHMENT/'
delimitador = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])
count = 0


def get_sku_Attachment_by_SKUID(id,count,delimitador):
	jsonF = {}
	if count >= delimitador:
		try:
			url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"+str(id)+"/attachment"
			headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
			response = requests.request("GET", url, headers=headers)
			string = response.text
			text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_ATTACHMENT/SKU_ATTACHMENT/"+str(count)+"_attachment.json", "w")
			text_file.write(string)
			text_file.close()
			print("Get_SKU_Attachment_by_SKUID.py Terminando: "+str(count))
		except:
			delimitador = count
			text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_ATTACHMENT/delimitador.txt", "w")
			text_file.write(str(delimitador))
			text_file.close()
			system("python3 Get_SKU_Attachment_by_SKUID.py")


def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_ATTACHMENT/id_sku.json','r')
	data_from_string = f_01.read()
	data_from_string = data_from_string.replace('"', '')
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		count += 1
		get_sku_Attachment_by_SKUID(i,count,delimitador)
	print(str(count)+" registro almacenado.")

operacion_fenix(count)

'''
DIR = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_ATTACHMENT/SKU_ATTACHMENT/'
countDir = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])

for x in range(countDir):
	try:
		registro += 1
		uri = "/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_ATTACHMENT/SKU_ATTACHMENT/"+str(registro)+"_attachment.json"
		f_03 = open (uri,'r')
		ids_string = f_03.read()
		formatoJson = json.loads(ids_string)
		listaID.append(formatoJson)
		print("Attachment Almacenados: " +str(registro))
	except:
		continue


string = json.dumps(listaID)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_ATTACHMENT/temp.json", "w")
text_file.write(string)
text_file.close() 

system("cat temp.json | jq -c '.[]' > tableSkuAttachment.json")

print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_ATTACHMENT/tableSku.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_sku_attachment'
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


