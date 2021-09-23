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


print("comenzando_trabajo") 

def get_list_order(creationDateFrom,creationDateTo):
	url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders"
	querystring = {"f_creationDate":"creationDate:[creationDate:["+creationDateFrom+"T02:00:00.000Z TO "+creationDateTo+"T01:59:59.999Z]]","f_hasInputInvoice":"false"}
	headers = {
		"Accept": "application/json",
		"Content-Type": "application/json",
		"X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
		"X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	animate() 
	cv.destroyAllWindows()
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders/ORDERS/JsonHistory/ordenes-"+creationDateFrom+"-"+creationDateTo+".json", "w")
	text_file.write(response.text)
	text_file.close() 

get_list_order("2021-01-01","2021-01-10")
print("Primera semana finalizada")
get_list_order("2021-01-10","2021-01-17")
print("Segunda semana finalizada")
get_list_order("2021-01-17","2021-01-24")
print("Tercera semana finalizada")
get_list_order("2021-01-24","2021-01-31")

print("Estado Finalizado")

