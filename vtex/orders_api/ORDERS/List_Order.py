import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

ids = 1
datatemp = []
datatemp2 = []

f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/HistoryListOrders/ListaOrdenesEnero.json','r')
data_from_string = f_01.read()

print(data_from_string)

'''
data_from = int(data_from_string)
data_to = int(data_to_string)
ids = int(ids_string)

client = bigquery.Client()

def get_productIFD(id,data_from,data_to,ids):
	datatemp = []
	url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds"
	querystring = {"categoryId":""+str(id)+"","_from":""+str(data_from)+"","_to":""+str(data_to)+""}
	headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	jsonF = json.loads(response.text)
	data = jsonF["data"]
	for total in data:
		datatemp.append(total)
	idsProducts = json.dumps(datatemp)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/"+str(ids)+"_productID_categoryID_"+str(id)+".json", "w")
	text_file.write(idsProducts)
	text_file.close()
	u_data_from = str(data_from)
	u_data_to = str(data_to)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/ultimoRegistroCargado_from.txt", "w")
	text_file.write(u_data_from)
	text_file.close()
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/ultimoRegistroCargado_to.txt", "w")
	text_file.write(u_data_to)
	text_file.close()
	if bool(data):
		data_from = data_from + 50
		data_to = data_to +50
		ids = ids + 1
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/ids.txt", "w")
		text_file.write(str(ids))
		text_file.close()
		print(str(ids)+" ID de producto agregado, categoryID= "+str(id))
		get_productIFD(id,data_from,data_to,ids)
	else:
		u_data_from = str(data_from)
		u_data_to = str(data_to)
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/ultimoRegistroCargado_from_categoryID_"+str(id)+".txt", "w")
		text_file.write(u_data_from)
		text_file.close()
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/ultimoRegistroCargado_to_categoryID_"+str(id)+".txt", "w")
		text_file.write(u_data_to)
		text_file.close()

QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_detail_category` ')
query_job = client.query(QUERY) 
rows = query_job.result()  

get_productIFD("703",data_from,data_to,ids)

#for row in rows:
#	get_productIFD(str(row.id),data_from,data_to,ids)
#	print("Lista De IDS Cargados")
#	break
'''