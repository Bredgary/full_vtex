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
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_address` AS SELECTGENERATE_UUID() id_address,DIM_CLIENT_document id_customer,DIM_SHIPPING_DATA_receiverName receiverName,DIM_SHIPPING_DATA_postalCode postalCode,DIM_SHIPPING_DATA_city city,DIM_SHIPPING_DATA_addressId addressId,DIM_SHIPPING_DATA_state state,DIM_SHIPPING_DATA_street street,DIM_SHIPPING_DATA_number number,DIM_SHIPPING_DATA_country country,DIM_SHIPPING_DATA_neighborhood neighborhood,DIM_SHIPPING_DATA_complement complement,DIM_SHIPPING_DATA_reference referenceFROM `shopstar-datalake.staging_zone.shopstar_vtex_order` ')
        query_job = client.query(QUERY)
        delete_duplicate()
    except:
        print("Consulta SQL no ejecutada")

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_address` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.dm_address`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

get_params()