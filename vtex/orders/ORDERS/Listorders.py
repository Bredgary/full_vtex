import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
print("comenzando_trabajo") 
for x in range(4):

#creationDateFrom = "2021-01-01"
#creationDateTo =   "2021-01-31"

def get_list_order(creationDateFrom,creationDateTo):
	url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders"
	querystring = {"f_creationDate":"creationDate:[creationDate:["++"T02:00:00.000Z TO "++"T01:59:59.999Z]]","f_hasInputInvoice":"false"}
	headers = {
		"Accept": "application/json",
		"Content-Type": "application/json",
		"X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
		"X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/lista.json", "w")
	text_file.write(string)
	text_file.close() 


get_list_order("2021-01-01","2021-01-10")
get_list_order("2021-01-10","2021-01-17")
get_list_order("2021-01-17","2021-01-24")
get_list_order("2021-01-24","2021-01-31")

