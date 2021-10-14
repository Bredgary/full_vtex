import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

client = bigquery.Client()
productList = []

QUERY = (
    'SELECT id  FROM `shopstar-datalake.landing_zone.shopstar_vtex_sku_ean_id`')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    productList.append(row.id)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU_EAN/id_ean.json", "w")
text_file.write(string)
text_file.close()

