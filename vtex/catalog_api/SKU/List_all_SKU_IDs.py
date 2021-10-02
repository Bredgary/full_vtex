import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
listaID = []
listIdProductAndContext =[]
count = 0
'''
f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/delimitador.txt','r')
data_from_string = f_01.read()
delimitador = int(data_from_string)

def get_list_sku(count,delimitador):
  if count >= delimitador:
    try:
      url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitids"
      querystring = {"page":""+str(count)+"","pagesize":"100"}
      headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
      response = requests.request("GET", url, headers=headers, params=querystring)
      jsonF = json.loads(response.text)
      string = json.dumps(jsonF)
      text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/IdSku/"+str(count)+"_IdSku.json", "w")
      text_file.write(string)
      text_file.close()
      print("Numero de registro: "+str(count))
    except:
      delimitador = count
      text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/delimitador.txt", "w")
      text_file.write(str(delimitador))
      text_file.close()
      system("python3 List_all_SKU_IDs.py")
  return jsonF


def operacion_fenix(count):
    for i in range(5000):
        count +=1
        list_sku = get_list_sku(count,delimitador)
        if not list_sku:
            break
    print(str(count)+" registro almacenado.")
    print(list_sku)

operacion_fenix(count)

'''

DIR = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/IdSku/'
countDir = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])

for x in range(countDir):
    count +=1
    uri = "/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/IdSku/"+str(count)+"_IdSku.json"
    f_03 = open (uri,'r')
    ids_string = f_03.read()
    formatoJson = json.loads(ids_string)
    listaID.append(formatoJson)
    #print("ID type: " +type(listaID))
    print("ID: " +listaID)
    break

#string = json.dumps(listaID)
#text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/listaRefId.json", "w")
#text_file.write(string)
#text_file.close() 
