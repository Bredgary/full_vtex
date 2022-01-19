import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join
import logging

class init:
  productList = []
  df = pd.DataFrame()
  
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
def get_order(email,reg):
  try:
    url = "https://mercury.vtexcommercestable.com.br/api/checkout/pub/profiles"
    querystring = {"email":""+str(email)+""}
    response = requests.request("GET", url, headers=init.headers, params=querystring)
    Fjson = json.loads(response.text)
    
    userProfileId = Fjson["userProfileId"]
    profileProvider = Fjson["profileProvider"]
    isComplete = Fjson["isComplete"]
    availableAccounts = Fjson["availableAccounts"]
    for x in availableAccounts:
        accountId = x["accountId"]
        paymentSystem = x["paymentSystem"]
        paymentSystemName = x["paymentSystemName"]
        cardNumber = x["cardNumber"]
        bin = x["bin"]
        expirationDate = x["expirationDate"]
        isExpired = x["isExpired"]
        df1 = pd.DataFrame({
            'email': email,
            'userProfileId': userProfileId,
            'profileProvider': profileProvider,
            'accountId': accountId,
            'paymentSystem': paymentSystem,
            'paymentSystemName': paymentSystemName,
            'cardNumber': cardNumber,
            'bin': bin,
            'expirationDate': expirationDate,
            'isExpired': isExpired,
            'isComplete': isComplete}, index=[0])
        init.df = init.df.append(df1)
    print("Registro: "+str(reg))
  except:
    print("No availableAccounts "+str(reg))

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_availableAccounts` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_availableAccounts`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

def clientJoin():
    try:
        print("Creacion shopstar_vtex_client_profile")
        client = bigquery.Client()
        QUERY = ('''
        CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_availableAccounts` AS 
        SELECT
        a.email,
        b.isComplete,
        b.isExpired,
        b.cardNumber,
        b.paymentSystem,
        b.expirationDate,
        b.paymentSystemName,
        b.accountId,
        b.bin,
        b.profileProvider,
        b.userProfileId   
        FROM `shopstar-datalake.staging_zone.shopstar_vtex_client` a
        LEFT JOIN 
        `shopstar-datalake.staging_zone.shopstar_vtex_availableAccounts` b
        ON
        a.email = b.email;''')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print("shopstar_vtex_client_profile actualizado exitosamente")
    except:
        print("Error shopstar_vtex_client_profile!!")
        logging.exception("message")

def run():
    try:
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_availableAccounts'
        
        client  = bigquery.Client(project = project_id)
        dataset  = client.dataset(dataset_id)
        table = dataset.table(table_id)
        
        if df.empty:
            print('DataFrame is empty!')
        else:
            client  = bigquery.Client(project = project_id)
            dataset  = client.dataset(dataset_id)
            table = dataset.table(table_id)
            job_config = bigquery.LoadJobConfig()
            #job_config.write_disposition = "WRITE_TRUNCATE"
            #job_config.autodetect = True
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job = client.load_table_from_json(json_object, table, job_config = job_config)
            print(job.result())
            clientJoin()
            delete_duplicate()
    except:
        print("Error.")
        logging.exception("message")
  
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT email  FROM `shopstar-datalake.staging_zone.shopstar_vtex_client`WHERE (email NOT IN (SELECT email FROM `shopstar-datalake.staging_zone.shopstar_vtex_availableAccounts`))')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.email,registro)
    run()

  
get_params()
