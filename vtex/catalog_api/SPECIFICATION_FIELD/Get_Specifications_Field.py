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



QUERY = (
    'SELECT Id FROM `shopstar-datalake.landing_zone.shopstar_vtex_product_specification` WHERE Id is not null')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
    productList.append(row.id)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SPECIFICATION_FIELD/SPECIFICATION_FIELD_ID.json", "w")
text_file.write(string)
text_file.close()
