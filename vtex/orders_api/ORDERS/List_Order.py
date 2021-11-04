import requests
import json
import os
import re
import datetime
from datetime import date
from datetime import timedelta
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

class Parametros:
	today = datetime.date.today()
	yesterday = today - datetime.timedelta(days=1)
	before_yesterday = today - datetime.timedelta(days=2)

def get_order_list(page):
	url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders?page="+str(page)+""
	querystring = {"f_creationDate":"creationDate:["+str(Parametros.before_yesterday)+"T02:00:00.000Z TO "+str(Parametros.yesterday)+"T01:59:59.999Z]","f_hasInputInvoice":"false"}
	headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	return FJson

def init():
	for x in range(30):
		FJson = get_order_list(x)
		if not FJson["list"]:
			break
		print(type(FJson))

init()

