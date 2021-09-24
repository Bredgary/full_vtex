import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

data_from = 1
data_to = 50
ids = 1

client = bigquery.Client()

def get_productIFD(id,data_from,data_to,ids):
	url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/GetProductAndSkuIds"
	querystring = {"categoryId":""+str(id)+"","_from":""+str(data_from)+"","_to":""+str(data_from)+""}
	headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	jsonF = json.loads(response.text)
	data = jsonF["data"]
	idsProducts = json.dumps(data)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/"+str(ids)+"_productID_From_"+str(data_from)+"_ProductID_To_"+str(data_to)+"_categoryID_"+str(id)+".json", "w")
	text_file.write(idsProducts)
	text_file.close()
	u_data_from = str(data_from)
	u_data_to = str(data_to)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/ultimoRegistroFromCargado__"+u_data_from+".json", "w")
	text_file.write(u_data_from)
	text_file.close()
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/ultimoRegistroToCargado__"+u_data_to+".json", "w")
	text_file.write(u_data_to)
	text_file.close()
	if data is not None:
		data_from = data_from + 50
		data_to = data_to +50
		ids = ids + 1
		print(str(ids)+" ID de producto agregado, categoryID= "+str(id))
		get_productIFD(id,data_from,data_to,ids)
	else:
		u_data_from = str(data_from)
		u_data_to = str(data_to)
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/ultimoRegistroFromCargado__"+u_data_from+".json", "w")
		text_file.write(u_data_from)
		text_file.close()
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/ultimoRegistroToCargado__"+u_data_to+".json", "w")
		text_file.write(u_data_to)
		text_file.close()

QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_detail_category` ')
query_job = client.query(QUERY) 
rows = query_job.result()  

for row in rows:
	get_productIFD(str(row.id),data_from,data_to,ids)
  