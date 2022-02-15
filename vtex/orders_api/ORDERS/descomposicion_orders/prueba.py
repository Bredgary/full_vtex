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

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema


def get_order(email,reg):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/checkout/pub/profiles"
        querystring = {"email":""+str(email)+""}
        response = requests.request("GET", url, headers=init.headers, params=querystring)
        Fjson = json.loads(response.text)
        
        userProfileId = Fjson["userProfileId"]
        profileProvider = Fjson["profileProvider"]
        isComplete = Fjson["isComplete"]
        availableAddresses = Fjson["availableAddresses"]
        for x in availableAddresses:
            addressType = x["addressType"]
            receiverName = x["receiverName"]
            addressId = x["addressId"]
            isDisposable = x["isDisposable"]
            postalCode = x["postalCode"]
            city = x["city"]
            state = x["state"]
            country = x["country"]
            street = x["street"]
            number = x["number"]
            neighborhood = x["neighborhood"]
            complement = x["complement"]
            reference = x["reference"]
            df1 = pd.DataFrame({
                'email': email,
                'userProfileId': userProfileId,
                'profileProvider': profileProvider,
                'isComplete': isComplete,
                'addressType': addressType,
                'receiverName': receiverName,
                'addressId': addressId,
                'isDisposable': isDisposable,
                'postalCode': postalCode,
                'city': city,
                'state': state,
                'country': country,
                'street': street,
                'number': number,
                'neighborhood': neighborhood,
                'complement': complement,
                'reference': reference}, index=[0])
            init.df = init.df.append(df1)
        print("Registro: "+str(reg))
        if df.empty:
            df1 = pd.DataFrame({
                'email': email}, index=[0])
            init.df = init.df.append(df1)
    except:
        df1 = pd.DataFrame({
            'email': email}, index=[0])
        init.df = init.df.append(df1)

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_client_availableAddresses` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_client_availableAddresses`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

def run():
    try:
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_client_availableAddresses'
        
        client  = bigquery.Client(project = project_id)
        dataset  = client.dataset(dataset_id)
        table = dataset.table(table_id)
        
        if df.empty:
            print('DataFrame is empty!')
        else:
            table_schema = {
                "name": "email",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "neighborhood",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "number",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "street",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "reference",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "country",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "addressType",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "state",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "isDisposable",
                "type": "BOOLEAN",
                "mode": "NULLABLE"
            },{
                "name": "addressId",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "city",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "receiverName",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "complement",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "profileProvider",
                "type": "STRING",
                "mode": "NULLABLE"
            },{
                "name": "postalCode",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },{
                "name": "isComplete",
                "type": "BOOLEAN",
                "mode": "NULLABLE"
            },{
                "name": "userProfileId",
                "type": "STRING",
                "mode": "NULLABLE"
            }
            
            try:
                client  = bigquery.Client(project = project_id)
                dataset  = client.dataset(dataset_id)
                table = dataset.table(table_id)
                job_config = bigquery.LoadJobConfig()
                job_config.schema = format_schema(table_schema)
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                job = client.load_table_from_json(json_object, table, job_config = job_config)
                print(job.result())
                delete_duplicate()
            except:
                client  = bigquery.Client(project = project_id)
                dataset  = client.dataset(dataset_id)
                table = dataset.table(table_id)
                job_config = bigquery.LoadJobConfig()
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                job = client.load_table_from_json(json_object, table, job_config = job_config)
                print(job.result())
                delete_duplicate()
    except:
        print("Error.")
        logging.exception("message")

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT email  FROM `shopstar-datalake.test.temp`;')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.email,registro)
        if registro == 2:
            run()
        if registro == 10:
            run()
        if registro == 20:
            run()
        if registro == 30:
            run()
        if registro == 40:
            run()
        if registro == 50:
            run()
        if registro == 100:
            run()
    run()
    
get_params()