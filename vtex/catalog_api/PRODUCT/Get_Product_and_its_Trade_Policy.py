import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
productList = []
listIdProductAndContext = []
listaIDS = []
registro = 0
'''
f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/delimitador3.txt','r')
data_from_string = f_01.read()
delimitador = int(data_from_string)
count = 0
 
def get_policy(id,count,delimitador):
    if count >= delimitador:
        try:
            url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/productget/"""+str(id)+""
            headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
            response = requests.request("GET", url, headers=headers)
            jsonF = json.loads(response.text)
            string = json.dumps(jsonF)
            text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/Trade_policy/"+str(count)+"_get_policy.json", "w")
            text_file.write(string)
            text_file.close()
            print("get_policy Terminando: "+str(count))
        except:
            delimitador = count
            text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/delimitador3.txt", "w")
            text_file.write(str(delimitador))
            text_file.close()
            system("python3 Get_Product_and_its_Trade_Policy.py")
    return "Finalizado"

def operacion_fenix(count):
    f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/id_producto.json','r')
    data_from_string = f_01.read()
    formatoJSon = json.loads(data_from_string)
    for i in formatoJSon:
        count +=1
        policy = get_policy(i,count,delimitador)
    print(str(count)+" registro almacenado.")
    print(policy)

operacion_fenix(count)

'''

DIR = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/Trade_policy/'
countDir = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])

for x in range(countDir):
    if x == 50:
        break
    registro +=1
    try:
        uri = "/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/SKU/"+str(registro)+"_get_policy.json"
        f_03 = open (uri,'r')
        ids_string = f_03.read()
        print(ids_string)
    except:
        continue
    
	#formatoJson = json.loads(ids_string)
	#listaID.append(formatoJson)
	#print("SKU Almacenados: " +str(registro))

'''
string = json.dumps(listaID)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/temp.json", "w")
text_file.write(string)
text_file.close() 

system("cat temp.json | jq -c '.[]' > tableTrade.json")


print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/tableTrade.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_policy_product_v2'
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
system("rm table.json")
system("rm lista.json")
print("finalizado")
'''
