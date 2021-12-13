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
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.product` AS SELECT GENERATE_UUID() id_product,visitorId id_visitor,product.productSKU productSKU,product.v2ProductName v2ProductName,product.v2ProductCategory v2ProductCategory,product.productVariant productVariant,product.productBrand productBrand,product.productRevenue productRevenue,product.localProductRevenue localProductRevenue,product.productPrice productPrice,product.localProductPrice localProductPrice,product.productQuantity productQuantity,product.productRefundAmount productRefundAmount,product.localProductRefundAmount localProductRefundAmount,product.isImpression isImpression,product.isClick isClick FROM`shopstar-datalake.191656782.ga_sessions_20211130`,UNNEST (hits) hits,UNNEST (hits.product) product')
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
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.product` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.product`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

get_params()