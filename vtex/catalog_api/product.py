import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
#import subprocess

client = bigquery.Client()
cantidad = 0

# Perform a query.
QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_category` ')


query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    string =  json.dumps(row.id)
    system("touch /home/bred_valenzuela/full_vtex/vtex/catalog_api/idsProducts.json")
    text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/idsProducts.json", "w")
    text_file.write(string)
    text_file.close()
    cantidad = cantidad+1
    print("Cant IDS Product: "+str(cantidad))

listIdCategory = text_file.read(idsProducts.json)

print("Finalizado")