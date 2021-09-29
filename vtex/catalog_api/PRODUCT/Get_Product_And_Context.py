import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
productList = []
count = 0
listIdProductAndContext = []
listaIDS = []
def get_contex(id):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/ProductGet/"""+str(id)+""
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    response = requests.request("GET", url, headers=headers)
    jsonF = json.loads(response.text)
    return jsonF

f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/lista.json','r')
data_from_string = f_01.read()

formatoJSon = json.loads(data_from_string)

for i in formatoJSon:
    count += 1
    listaIDS.append(str(count),str(i)))

    #print(str(count)+" ID Producto: "+str(i))

#def replace_blank_dict(d):
#    if not d:
#        return None
#    if type(d) is list:
#        for list_item in d:
#            if type(list_item) is dict:
#                for k, v in list_item.items():
#                    list_item[k] = replace_blank_dict(v)
#    if type(d) is dict:
#        for k, v in d.items():
#            d[k] = replace_blank_dict(v)
#    return d

#QUERY = (
#    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_product_v2` ')
#query_job = client.query(QUERY)  # API request
#rows = query_job.result()  # Waits for query to finish

#for row in rows:
#    listIdProductAndContext.append(row.id)
    #temp = get_contex(str(row.id))
    #productList.append(temp)
    #count +=1
    #print(str(count)+" Registro almacenado")

#string = json.dumps(listIdProductAndContext)
#text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/lista.json", "w")
#text_file.write(string)
#text_file.close() 

'''
for order in productList:
    for k, v in order.items():
        order[k] = replace_blank_dict(v)


string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/lista.json", "w")
text_file.write(string)
text_file.close() 



system("cat lista.json | jq -c '.[]' > table.json")

print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/table.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_context_product_v2'
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
system("rm lista.json")
system("rm table.json")
print("finalizado")
'''

