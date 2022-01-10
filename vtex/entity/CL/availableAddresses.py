import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join

class init:
  productList = []
  df = pd.DataFrame()
  df2 = pd.DataFrame()
  
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema


def get_order(email):
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
    except:
        print("Vacio")

def run():
    try:
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        table_schema = [
        {
            "name": "email",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "userProfileId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "profileProvider",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "addressType",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "receiverName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "addressId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "isDisposable",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "postalCode",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "city",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "state",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "country",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "street",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "number",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "neighborhood",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "complement",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "reference",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "isComplete",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        }]
        
        project_id = '999847639598'
        dataset_id = 'test'
        table_id = 'shopstar_vtex_client_availableAddresses'
        try:
            client  = bigquery.Client(project = project_id)
            dataset  = client.dataset(dataset_id)
            table = dataset.table(table_id)
            job_config = bigquery.LoadJobConfig()
            job_config.schema = format_schema(table_schema)
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job = client.load_table_from_json(json_object, table, job_config = job_config)
            print(job.result())
        except:
            client  = bigquery.Client(project = project_id)
            dataset  = client.dataset(dataset_id)
            table = dataset.table(table_id)
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = "WRITE_TRUNCATE"
            job_config.autodetect = True
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job = client.load_table_from_json(json_object, table, job_config = job_config)
    except:
        print("Vacio")

def run2(email):
    df = init.df
    if df.empty:
        df = pd.DataFrame({
            'email': email,
            'Data': False}, index=[0])
        
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        project_id = '999847639598'
        dataset_id = 'test'
        table_id = 'shopstar_vtex_client_availableAddresses_No_Data'
        client  = bigquery.Client(project = project_id)
        dataset  = client.dataset(dataset_id)
        table = dataset.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = "WRITE_TRUNCATE"
        job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job = client.load_table_from_json(json_object, table, job_config = job_config)
        print(job.result())
        
        
def get_params():
  print("Cargando consulta")
  client = bigquery.Client()
  QUERY = ('SELECT DISTINCT email FROM `shopstar-datalake.cons_zone.dm_customer`WHERE (email NOT IN (SELECT email FROM `shopstar-datalake.test.shopstar_vtex_client_availableAddresses`))')
  query_job = client.query(QUERY)
  rows = query_job.result()
  registro = 0
  for row in rows:
    registro += 1
    get_order(row.email)
    print("Registro: "+str(registro))
    if registro == 1:
        run()
        run2(row.email)
    if registro == 300:
        run()
        run2(row.email)
    if registro == 400:
        run()
        run2(row.email)
    if registro == 500:
        run()
        run2(row.email)
    if registro == 600:
        run()
        run2(row.email)
    if registro == 700:
        run()
        run2(row.email)
    if registro == 800:
        run()
        run2(row.email)
    if registro == 900:
        run()
        run2(row.email)
    if registro == 1000:
        run()
        run2(row.email)
    if registro == 1100:
        run()
        run2(row.email)
    if registro == 1200:
        run()
        run2(row.email)
    if registro == 1300:
        run()
        run2(row.email)
    if registro == 1400:
        run()
        run2(row.email)
    if registro == 1500:
        run()
        run2(row.email)
    if registro == 10000:
        run()
        run2(row.email)
    if registro == 15000:
        run()
        run2(row.email)
    if registro == 20000:
        run()
        run2(row.email)
    if registro == 25000:
        run()
        run2(row.email)
    if registro == 30000:
        run()
        run2(row.email)
    if registro == 35000:
        run()
        run2(row.email)
    if registro == 40000:
        run()
        run2(row.email)
    if registro == 45000:
        run()
        run2(row.email)
    if registro == 50000:
        run()
        run2(row.email)
    if registro == 55000:
        run()
        run2(row.email)
    if registro == 60000:
        run()
        run2(row.email)
    if registro == 65000:
        run()
        run2(row.email)
    if registro == 70000:
        run()
        run2(row.email)
    if registro == 75000:
        run()
        run2(row.email)
    if registro == 80000:
        run()
        run2(row.email)
    if registro == 85000:
        run()
        run2(row.email)
    if registro == 90000:
        run()
        run2(row.email)
  run()
  run2(row.email)

  
get_params()
