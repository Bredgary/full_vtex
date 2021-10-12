import os, sys
import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery

productList = []
client = bigquery.Client()

QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_collection_beta`')
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    productList.append(row.id)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/COLLECTION/collection.json", "w")
text_file.write(string)
text_file.close()