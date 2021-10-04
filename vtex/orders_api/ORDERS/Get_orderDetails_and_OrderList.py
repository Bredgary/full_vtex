import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

day = datetime.today().strftime('%d')
mouth = datetime.today().strftime('%m')
year = datetime.today().strftime('%y')

limite = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30]
formatoJson = {}
formatoList = []
listDetails = []
list_order = []
order = {}
count = 0
mouth = int(mouth) - 1
dayFrom = int(day) - 32
dayTo = int(day) - 31

def replace_blank_dict(d):
    if not d:
        return None
    if type(d) is list:
        for list_item in d:
            if type(list_item) is dict:
                for k, v in list_item.items():
                    list_item[k] = replace_blank_dict(v)
    if type(d) is dict:
        for k, v in d.items():
            d[k] = replace_blank_dict(v)
    return d

def get_order(ids):
    url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(ids)+""
    headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    response = requests.request("GET", url, headers=headers)
    formatoJ = json.loads(response.text)
    for k, v in formatoJ.items():
        order[k] = replace_blank_dict(v)
    listDetails.append(formatoJ)

def get_list(pag):
    url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/?page="+str(pag)+""
    querystring = {"f_creationDate":"creationDate:[20"+str(year)+"-"+str(mouth)+"-"+str(dayFrom)+"T02:00:00.000Z TO 20"+str(year)+"-"+str(mouth)+"-"+str(dayTo)+"T01:59:59.999Z]","f_hasInputInvoice":"false"}
    print("Cargando Fecha: 20"+str(year)+"-"+str(mouth)+"-"+str(dayFrom)+" al 20"+str(year)+"-"+str(mouth)+"-"+str(dayTo)+"")
    headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    formatoJson = json.loads(response.text)
    lista = formatoJson["list"]
    for i in lista:
        get_order(i["orderId"])
    return formatoJson

for i in limite:
    count = count + 1
    print(str(count)+" Pagina recorrida")
    x = get_list(i)
    break
    if bool(x["list"]):
        list_order.append(x["list"])
    else:

