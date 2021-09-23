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

done = 'false'
def animate():
    while done == 'false':
        sys.stdout.write('\rloading |')
        time.sleep(0.1)
        sys.stdout.write('\rloading /')
        time.sleep(0.1)
        sys.stdout.write('\rloading -')
        time.sleep(0.1)
        sys.stdout.write('\rloading \\')
        time.sleep(0.1)
    sys.stdout.write('\rDone!     ')
animate()
done = 'false'
contador = 0
print("comenzando_trabajo") 

def get_list_order(creationDateFrom,creationDateTo):
	print("Desde: "+creationDateFrom)
	print("Hasta: "+creationDateTo)
	url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders"
	querystring = {"f_creationDate":"creationDate:[creationDate:["+creationDateFrom+"T02:00:00.000Z TO "+creationDateTo+"T01:59:59.999Z]]","f_hasInputInvoice":"false"}
	headers = {
		"Accept": "application/json",
		"Content-Type": "application/json",
		"X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
		"X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	animate() 
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders/ORDERS/JsonHistory/ordenes-"+creationDateFrom+"-"+creationDateTo+".json", "w")
	text_file.write(response.text)
	text_file.close()
	print("Dia: " + str(contador + 1))

print("Primera Semana")
get_list_order("2021-01-01","2021-01-02")
get_list_order("2021-01-02","2021-01-03")
get_list_order("2021-01-03","2021-01-04")
get_list_order("2021-01-04","2021-01-05")
get_list_order("2021-01-05","2021-01-06")
get_list_order("2021-01-06","2021-01-07")
get_list_order("2021-01-07","2021-01-08")
get_list_order("2021-01-08","2021-01-09")

print("Estado Finalizado")

