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
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.hits` AS SELECT GENERATE_UUID() id_hit,h.hitNumber hitNumber,h.time time,h.hour hour,h.minute minute,h.isSecure isSecure,h.isInteraction isInteraction,h.isEntrance isEntrance,h.isExit isExit,h.referer referer,h.transaction.transactionId id_transaction,h.type type,h.dataSource dataSource,h.uses_transient_token uses_transient_token FROM `shopstar-datalake.191656782.ga_sessions_20211130`,UNNEST(hits) as h')

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
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.hits` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.hits`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

get_params()