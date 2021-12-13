import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join

class init:
    df = pd.DataFrame()

def get_params():
    try:
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_phone` AS SELECT GENERATE_UUID() id_phone,GENERATE_UUID() id_typePhone,DIM_CLIENT_document id_customer,DIM_CLIENT_phone phoneNumber FROM `shopstar-datalake.staging_zone.shopstar_vtex_order` ')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
        delete_duplicate()
    except:
        print("Consulta SQL no ejecutada")

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_phone` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.dm_phone`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

get_params()