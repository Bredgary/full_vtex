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
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.totals` AS SELECT GENERATE_UUID() id_Metric,visitId id_visitor,totals.visits visits,totals.hits hits,totals.pageviews pageviews,totals.timeOnSite timeOnSite,totals.bounces bounces,    totals.transactions transactions,    totals.transactionRevenue transactionRevenue,    totals.newVisits newVisits,    totals.screenviews screenviews,    totals.uniqueScreenviews uniqueScreenviews,totals.timeOnScreen timeOnScreen,totals.totalTransactionRevenue totalTransactionRevenue,totals.sessionQualityDim sessionQualityDim FROM `shopstar-datalake.191656782.ga_sessions_20211130`')

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
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.totals` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.totals`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

get_params()