import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/collection/search"
querystring = {"page":"1","pageSize":"1","orderByAsc":"true"}
headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
response = requests.request("GET", url, headers=headers, params=querystring)
Json = json.loads(response.text)
paging = Json["paging"]
total = int(paging["total"])
pages = int(paging["pages"])
listItem = []

def get_collection_beta(page,headers,total):
	url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/collection/search"
	querystring = {"page":""+str(page)+"","pageSize":""+str(total)+"","orderByAsc":"true"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	listItem.append(FJson["items"])



for x in range(pages):
	get_collection_beta(x,headers,total)

