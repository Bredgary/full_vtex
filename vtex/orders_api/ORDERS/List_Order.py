import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

dia = datetime.today().strftime('%d')
dia1 = int(dia) - 25
dia2 = int(dia) - 24

limite = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30]
temporal = {}
list_orders=[]
dict = defaultdict(list)
formatoJson = {}
formatoDict = {}
count = 0

def get_list(pag):
	url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/?page="+str(pag)+""
	querystring = {"f_creationDate":"creationDate:[2021-09-"+str(dia1)+"T02:00:00.000Z TO 2021-09-"+str(dia2)+"T01:59:59.999Z]","f_hasInputInvoice":"false"}
	headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	formatoJson = json.loads(response.text)
	return formatoJson

for i in limite:
    x = get_list(i)
    if bool(x["list"]):
        for k, v in chain(x.items()):
            dict[k].append(v)
    else:
        break

formatoDict.update(dict)

formatoJson = json.loads(formatoDict.text)
string = json.dumps(formatoJson)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/dictCompuesto.json", "w")
text_file.write(string)
text_file.close()

system("cat dictCompuesto.json | jq -c '.[]' > tabla.json")

'''
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