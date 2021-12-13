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
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.transaction` AS SELECT GENERATE_UUID() id_transaction,h.transaction.transactionId transactionId,h.transaction.transactionRevenue transactionRevenue,h.transaction.transactionTax transactionTax,h.transaction.transactionShipping transactionShipping,h.transaction.affiliation affiliation,h.transaction.currencyCode currencyCode,h.transaction.localTransactionRevenue localTransactionRevenue,h.transaction.localTransactionTax localTransactionTax,h.transaction.localTransactionShipping localTransactionShipping,h.transaction.transactionCoupon transactionCouponFROM `shopstar-datalake.191656782.ga_sessions_20211130`,UNNEST(hits) as h')

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
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.transaction` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.transaction`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

get_params()