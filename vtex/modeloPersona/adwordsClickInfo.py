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
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.adwordsClickInfo` AS SELECT GENERATE_UUID() id_adwordsClickInfo,trafficSource.adwordsClickInfo.adGroupId adGroupId,trafficSource.adwordsClickInfo.creativeId creativeId,trafficSource.adwordsClickInfo.criteriaId criteriaId,trafficSource.adwordsClickInfo.page page,trafficSource.adwordsClickInfo.slot slot,trafficSource.adwordsClickInfo.criteriaParameters criteriaParameters,trafficSource.adwordsClickInfo.gclId gclId,trafficSource.adwordsClickInfo.customerId customerId,trafficSource.adwordsClickInfo.adNetworkType adNetworkType,trafficSource.adwordsClickInfo.isVideoAd isVideoAd FROM `shopstar-datalake.191656782.ga_sessions_20211130` ')
        query_job = client.query(QUERY)
        delete_duplicate()
    except:
        print("Consulta SQL no ejecutada")

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.adwordsClickInfo` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.adwordsClickInfo`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

get_params()