#!/usr/bin/python
# -*- coding: latin-1 -*-
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
    #try:
        client = bigquery.Client()
        QUERY = ('SELECT orderId,EnableInferItems,cfop,restitutions,courierStatus,embeddedInvoice,invoiceNumber,invoiceKey,issuanceDate,trackingUrl,invoiceUrl,type,invoiceValue,courier FROM `shopstar-datalake.staging_zone.shopstar_vtex_order` ')
        query_job = client.query(QUERY)
        rows = query_job.result()
        for row in rows:
            df1 = pd.DataFrame({
                'orderId': row.orderId,
                'EnableInferItems': row.EnableInferItems,
                'cfop': row.cfop,
                'restitutions': row.restitutions,
                'email': row.courierStatus,
                'courierStatus': row.email,
                'embeddedInvoice': row.embeddedInvoice,
                'invoiceNumber': row.invoiceNumber,
                'invoiceKey': row.invoiceKey,
                'issuanceDate': row.issuanceDate,
                'trackingUrl': row.trackingUrl,
                'invoiceUrl': row.invoiceUrl,
                'type': row.type,
                'invoiceValue': row.invoiceValue,
                'courier': row.courier}, index=[0])
            init.df = init.df.append(df1)
    #except:
    #    print("Consulta SQL no ejecutada")

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_package` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.dm_package`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

def run():
    #try:
        get_params()
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        project_id = '999847639598'
        dataset_id = 'cons_zone'
        table_id = 'dm_package'
        
        client  = bigquery.Client(project = project_id)
        dataset  = client.dataset(dataset_id)
        table = dataset.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = "WRITE_TRUNCATE"
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.autodetect = True
        job = client.load_table_from_json(json_object, table, job_config = job_config)
        print(job.result())
        delete_duplicate()
    #except:
    #    print("Error")
    
run()