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
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.trafficSource` AS SELECT GENERATE_UUID() id_trafficSource,visitId id_visitor,trafficSource.referralPath referralPath,trafficSource.campaign campaign,    trafficSource.source source,    trafficSource.medium medium,    trafficSource.keyword keyword,trafficSource.adContent adContent,GENERATE_UUID() adwordsClickInfo,trafficSource.isTrueDirect isTrueDirect,trafficSource.campaignCode campaignCode FROM `shopstar-datalake.191656782.ga_sessions_20211130` ')
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
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.trafficSource` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.trafficSource`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

get_params()