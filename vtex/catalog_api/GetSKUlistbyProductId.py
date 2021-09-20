import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
productList = [] 
temp = ""
headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}


def get_sku_list(id,headers):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitByProductId/"""+str(id)+""
    response = requests.request("GET", url, headers=headers)
    formatoJson = json.loads(response.text)
    productList.append(formatoJson)
    

QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_product` ')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    get_sku_list(str(row.id),headers)
    break


string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/lista1.json", "w")
text_file.write(string)
text_file.close() 
system("cat lista1.json | jq -c '.[]' > lista.json")


print("Cargando a BigQuery")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/lista.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_list_sku'
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("Id", "INTEGER"),
        bigquery.SchemaField("ProductId", "INTEGER"),
    ],
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)
with open(filename, "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location="southamerica-east1",  # Must match the destination dataset location.
    job_config=job_config,)  # API request
job.result()  # Waits for table load to complete.
print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
system("rm lista.json")
system("rm context.json")
print("finalizado")