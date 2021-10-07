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
listaID = []
registro = 0

'''

f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/delimitador2.txt','r')
data_from_string = f_01.read()
delimitador = int(data_from_string)
count = 0

def skuandproduct(id,count,delimitador):
    if count >= delimitador:
        try:
            url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pub/products/variations/"""+str(id)+""
            headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
            response = requests.request("GET", url, headers=headers)
            jsonF = json.loads(response.text)
            string = json.dumps(jsonF)
            text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/SKUs_by_id/"+str(count)+"_skus_by_Product.json", "w")
            text_file.write(string)
            text_file.close()
            print("Get_Product_SKUs_by_ProductID Terminando: "+str(count))
        except:
            delimitador = count
            text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/delimitador2.txt", "w")
            text_file.write(str(delimitador))
            text_file.close()
            system("python3 Get_Product_SKUs_by_ProductID.py")
    return "Finalizado"

def operacion_fenix(count):
    f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/id_producto.json','r')
    data_from_string = f_01.read()
    formatoJSon = json.loads(data_from_string)
    for i in formatoJSon:
        count +=1
        sku = skuandproduct(i,count,delimitador)
    print(str(count)+" registro almacenado.")
    print(sku)

operacion_fenix(count)


'''

DIR = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/SKUs_by_id/'
countDir = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])

for x in range(countDir):
    try:
        registro +=1
        uri = "/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/SKUs_by_id/"+str(registro)+"_skus_by_Product.json"
        f_03 = open (uri,'r')
        ids_string = f_03.read()
        print(ids_string)
        #formatoJson = json.loads(ids_string)
        #listaID.append(formatoJson)
        break
    except:
        continue
'''
print("SKU Almacenados: " +str(registro))


string = json.dumps(listaID)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/temp.json", "w")
text_file.write(string)
text_file.close() 

system("cat temp.json | jq -c '.[]' > tableSku.json")


print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/tableSku.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_sku_by_product'
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