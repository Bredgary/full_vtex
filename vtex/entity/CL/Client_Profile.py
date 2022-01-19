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
        
        userProfile = Fjson["userProfile"]
        email = userProfile["email"]
        firstName = userProfile["firstName"]
        lastName = userProfile["lastName"]
        document = str(userProfile["document"])
        documentType = userProfile["documentType"]
        phone = userProfile["phone"]
        corporateName = userProfile["corporateName"]
        tradeName = userProfile["tradeName"]
        corporateDocument = userProfile["corporateDocument"]
        stateInscription = userProfile["stateInscription"]
        corporatePhone = userProfile["corporatePhone"]
        isCorporate = userProfile["isCorporate"]
        profileCompleteOnLoading = userProfile["profileCompleteOnLoading"]
        profileErrorOnLoading = userProfile["profileErrorOnLoading"]
        customerClass = userProfile["customerClass"]
        
        df1 = pd.DataFrame({
            'userProfileId': userProfileId,
            'profileProvider': profileProvider,
            'isComplete': isComplete,
            'email': email,
            'firstName': firstName,
            'lastName': lastName,
            'document': str(document),
            'documentType': documentType,
            'phone': phone,
            'corporateName': corporateName,
            'tradeName': tradeName,
            'corporateDocument': corporateDocument,
            'stateInscription': stateInscription,
            'corporatePhone': corporatePhone,
            'isCorporate': isCorporate,
            'profileCompleteOnLoading': profileCompleteOnLoading,
            'profileErrorOnLoading': profileErrorOnLoading,
            'customerClass': customerClass}, index=[0])
        init.df = init.df.append(df1)
        print("Registro: "+str(reg))
    except:
        print("No data profile "+str(reg))
        

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_client_profile` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_client_profile`')
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
        CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_client_profile` AS 
        SELECT
        a.email,
        b.profileErrorOnLoading,
        b.corporateName,
        b.phone,
        b.isCorporate,
        b.stateInscription,
        b.document,
        b.documentType,
        b.lastName,
        b.profileCompleteOnLoading,
        b.tradeName,
        b.firstName,
        b.profileProvider,
        b.corporatePhone,
        b.isComplete,
        b.customerClass,
        b.corporateDocument,
        b.userProfileId    
        FROM `shopstar-datalake.staging_zone.shopstar_vtex_client` a
        LEFT JOIN 
        `shopstar-datalake.staging_zone.shopstar_vtex_client_profile` b
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
        table_id = 'shopstar_vtex_client_profile'
        
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
    QUERY = ('SELECT email  FROM `shopstar-datalake.staging_zone.shopstar_vtex_client`WHERE (email NOT IN (SELECT email FROM `shopstar-datalake.staging_zone.shopstar_vtex_client_profile`))')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.email,registro)
    run()
  
get_params()
