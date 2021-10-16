import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

client = bigquery.Client()

QUERY = (
    'SELECT email FROM `shopstar-datalake.landing_zone.shopstar_vtex_search_documents` WHERE email is not null')
query_job = client.query(QUERY)  
rows = query_job.result()  

for row in rows:
    productList.append(row.email)

string = json.dumps(productList)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SPECIFICATION_FIELD/SPECIFICATION_FIELD_ID_2.json", "w")
text_file.write(string)
text_file.close()
