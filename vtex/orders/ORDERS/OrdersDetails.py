import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
print("comenzando_trabajo") 
#================================================TOTAL DE PAGINAS===============================================================
headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
querystring = {"f_creationDate":"creationDate:[2021-01-01T02:00:00.000Z TO 2021-03-31T01:59:59.999Z]","f_hasInputInvoice":"false"}
urlPag = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders"
responsePag = requests.request("GET", urlPag, headers=headers, params=querystring)
formJson = json.loads(responsePag.text)
paging = formJson["paging"]
total = paging["total"]
tableDetails = []
dataorderDe=[]
OrderF = []

contador = 0
#================================================TOTAL DE PAGINAS===============================================================

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

#================================================Obtener Orden Por ID============================================================
def insertar(ids,headers):
  urlDetail = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+ids+""
  response = requests.request("GET", urlDetail, headers=headers)
  result = re.sub('[!@#$|]', '', response.text)
  data = json.loads(result)
  print("Total de registros: "+str(paging["total"]))
  return data
#================================================Obtener Orden Por ID============================================================


for i in range(total):
  i =+1
  OrderId = []
  #print("===========================================================================")
  url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders?page="+str(i)+""
  response = requests.request("GET", url, headers=headers, params=querystring)
  numeroPaginas = i
  formatoJson = json.loads(response.text)
  listOrder = formatoJson["list"]
  for ids in listOrder:
    OrderId.append(ids["orderId"])
  for x in OrderId:
    orderDe = insertar(str(x),headers)
    OrderF.append(orderDe)
    contador = contador + 1
    string =  json.dumps(OrderF)
    print("Registros almacenados "+str(contador)+" de "+str(total))
    for order in OrderF:
        for k, v in order.items():
            order[k] = replace_blank_dict(v)
    text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders/respaldo.json", "w")
    text_file.write(string)
    text_file.close() 

formatoOrder =  json.dumps(OrderF)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders/temp.json", "w")
text_file.write(formatoOrder)
text_file.close() 
system("cat temp.json | jq -c '.[]' > DetailOrdersFinal.json")

print("Comenzando la ingesta")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/orders/DetailOrdersFinal.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_orders_details'
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.autodetect = True
with open(filename, "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location="southamerica-east1",  # Must match the destination dataset location.
    job_config=job_config,)  # API request
job.result()  # Waits for table load to complete.
print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
system("rm DetailOrdersFinal.json")
system("rm temp.json")
print("finalizado")