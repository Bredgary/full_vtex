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

f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/delimitador.txt','r')
data_from_string = f_01.read()
delimitador = int(data_from_string)
count = 0
mensajeError = {'Message': 'The request is invalid.'}


def get_sku(id,count,delimitador):
	if count >= delimitador:
		#try:
		url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"""+str(id)+""
		headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
		response = requests.request("GET", url, headers=headers)
		jsonF = json.loads(response.text)
		if jsonF != mensajeError:
			string = json.dumps(jsonF)
			text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/SKU/"+str(count)+"_sku.json", "w")
			text_file.write(string)
			text_file.close()
			print("Numero de registro: "+str(count))
		else:
			print("Vacio")
		#except:
		#	delimitador = count
		#	text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/delimitador.txt", "w")
		#	text_file.write(str(delimitador))
		#	text_file.close()
		#	system("python3 Get_SKU.py")
	return "Finalizado"


def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/id_sku.json','r')
	data_from_string = f_01.read()
	formatoJSon = json.loads(data_from_string)
	for i in formatoJSon:
		count += 1
		sku = get_sku(i,count,delimitador)
	print(str(count)+" registro almacenado.")
	print(sku)


operacion_fenix(count)

'''

QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_product_v2` ')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish


for row in rows:
    listIdProductAndContext.append(row.id)
    
string = json.dumps(listIdProductAndContext)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/id_producto.json", "w")
text_file.write(string)
text_file.close() 


f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/lista.json','r')
data_from_string = f_01.read()

formatoJSon = json.loads(data_from_string)

for i in formatoJSon:
        xx = get_contex(i)
        listaIDS.append(xx)
        count += 1
        print(str(count)+" registro almacenado "+str(i))

     
string = json.dumps(listaIDS)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/respaldo.json", "w")
text_file.write(string)
text_file.close()        


for i in formatoJSon:
    try:
        xx = get_context(i)
        listaIDS.append(xx)
        print(str(count)+" registro almacenado "+str(i))
    except:
        print(str(count)+" ID Producto: "+str(i))
        string = json.dumps(listaIDS)
        text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/respaldo.json", "w")
        text_file.write(string)
        text_file.close()
        text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/count.txt", "w")
        text_file.write(str(count))
        text_file.close() 
        text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/ids.txt", "w")
        text_file.write(str(i))
        text_file.close() 


string = json.dumps(listaIDS)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/lista2.json", "w")
text_file.write(string)
text_file.close() 
print("Finalizado")

#QUERY = (
#    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_product_v2` ')
#query_job = client.query(QUERY)  # API request
#rows = query_job.result()  # Waits for query to finish

#for row in rows:
#    listIdProductAndContext.append(row.id)
    #temp = get_contex(str(row.id))
    #productList.append(temp)
    #count +=1
    #print(str(count)+" Registro almacenado")

#string = json.dumps(listIdProductAndContext)
#text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/lista.json", "w")
#text_file.write(string)
#text_file.close() 



system("cat lista.json | jq -c '.[]' > table.json")

print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/table.json'
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
system("rm lista.json")
system("rm table.json")
print("finalizado")
'''

