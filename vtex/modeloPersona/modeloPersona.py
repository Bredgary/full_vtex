import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join
import logging


def delete_duplicate():
  try:
    print("Eliminando duplicados")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_order_item` AS 
    SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_order_item`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print(rows)
  except:
    print("Consulta SQL no ejecutada")
    logging.exception("message")
    
delete_duplicate()