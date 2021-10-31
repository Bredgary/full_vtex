import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()
listaID = []
listIdSkuAndContext =[]
registro=0
'''
f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/delimitador1.txt','r')
data_from_string = f_01.read()
delimitador = int(data_from_string)
count = 0
mensajeError = '"'+"SKU not found"+'"'


def get_sku(id,count,delimitador):
	jsonF = {}
	if count >= delimitador:
		try:
			url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyid/"""+str(id)+""
			querystring = {"sc":"1"}
			headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
			response = requests.request("GET", url, headers=headers, params=querystring)
			jsonF = json.loads(response.text)
			if jsonF != mensajeError:
				string = json.dumps(jsonF)
				text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/SKUContext/"+str(count)+"_sku.json", "w")
				text_file.write(string)
				text_file.close()
				print("Get_SKU_and_context.py Terminando: "+str(count))
		except:
			delimitador = count
			text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/delimitador1.txt", "w")
			text_file.write(str(delimitador))
			text_file.close()
			system("python3 Get_SKU_and_context.py")


def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/id_sku.json','r')
	data_from_string = f_01.read()
	data_from_string = data_from_string.replace('"', '')
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		count += 1
		get_sku(i,count,delimitador)
	print(str(count)+" registro almacenado.")

operacion_fenix(count)

'''

DIR = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/SKUContext'
countDir = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])

for x in range(countDir):
	try:
		registro += 1
		uri = "/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/SKUContext/"+str(registro)+"_sku.json"
		f_03 = open (uri,'r')
		ids_string = f_03.read()
		formatoJson = json.loads(ids_string)
		listaID.append(formatoJson)
		print("sku contexts Almacenados: " +str(registro))
	except:
		string = json.dumps(listaID)
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/"+str(registro)+"temp.json", "w")
		text_file.write(string)
		text_file.close() 

string = json.dumps(listaID)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/skuF.json", "w")
text_file.write(string)
text_file.close() 

