#!/usr/bin/python
# -*- coding: UTF-8 -*-
import requests
import json
import os
import re
from datetime import datetime
from os import system
import time
import sys

print("comenzando_trabajo") 
#registro = 0

def get_list_order():
	#registro += 1
	url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders"
	querystring = {"f_creationDate":"creationDate:[2021-01-01T02:00:00.000Z TO 2021-01-31T01:59:59.999Z]","f_hasInputInvoice":"false"}
	headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	jsonF = json.loads(response.text)
	data = jsonF["list"]
	idsProducts = json.dumps(data)
	print(data)
	print(type(data))
	#text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/HistoryListOrders/"+str(registro)+"_mes_enero.json", "w")
	#text_file.write(idsProducts)
	#text_file.close()

get_list_order()

print("Estado Finalizado!!")

