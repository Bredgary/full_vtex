import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
#import subprocess


listIdCategory = []
client = bigquery.Client()
cantidad = 0

# Perform a query.
QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_category` ')


query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

system("touch /home/bred_valenzuela/full_vtex/vtex/catalog_api/idsProducts.json")
for row in rows:
    listIdCategory.append(row.id)
    cantidad = cantidad+1
    print("Cant IDS Product: "+str(cantidad))

string =  json.dumps(listIdCategory)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/idsProducts.json", "w")
text_file.write(string)
text_file.close()

idsCategory=open("idsProducts.json","r")
print(idsCategory.read())



print("Finalizado")