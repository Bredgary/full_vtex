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

def decrypt_email(email):
  try:
    url = "https://conversationtracker.vtex.com.br/api/pvt/emailMapping?an=mercury&alias="+email+""
    response = requests.request("GET", url, headers=init.headers)
    formatoJ = json.loads(response.text)
    return formatoJ["email"]
  except:
    print(str(email))
    
def get_order(id):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        
        clientProfileData = Fjson["clientProfileData"]
        client_email = clientProfileData["email"]
        client_email = decrypt_email(str(client_email))
        firstName = clientProfileData["firstName"]
        lastName = clientProfileData["lastName"]
        documentType = clientProfileData["documentType"]
        document = clientProfileData["document"]
        phone = clientProfileData["phone"]
        corporateName = clientProfileData["corporateName"]
        tradeName = clientProfileData["tradeName"]
        corporateDocument = clientProfileData["corporateDocument"]
        stateInscription = clientProfileData["stateInscription"]
        corporatePhone = clientProfileData["corporatePhone"]
        isCorporate = clientProfileData["isCorporate"]
        userProfileId = clientProfileData["userProfileId"]
       
        df1 = pd.DataFrame({
            'orderId': id,
            'email': client_email,
            'firstName': firstName,
            'lastName': lastName,
            'documentType': documentType,
            'document': document,
            'phone': phone,
            'corporateName': corporateName,
            'tradeName': tradeName,
            'corporateDocument': corporateDocument,
            'stateInscription': stateInscription,
            'corporatePhone': corporatePhone,
            'isCorporate': isCorporate,
            'userProfileId': userProfileId}, index=[0])
        init.df = init.df.append(df1)
    except:
        print("vacio") 
            
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_order_client` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_order_client`')
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
        
        table_schema = [
            {
              "name": "corporatePhone",
              "type": "STRING",
              "mode": "NULLABLE"
        },{
            "name": "isCorporate",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "document",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "stateInscription",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "userProfileId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "corporateDocument",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "orderId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "corporateName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "phone",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "documentType",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "lastName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "tradeName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "firstName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "email",
            "type": "STRING",
            "mode": "NULLABLE"
        }]
        
        project_id = '999847639598'
        dataset_id = 'test'
        table_id = 'shopstar_order_client'
        
        client  = bigquery.Client(project = project_id)
        dataset  = client.dataset(dataset_id)
        table = dataset.table(table_id)
        job_config = bigquery.LoadJobConfig()
        #job_config.write_disposition = "WRITE_TRUNCATE"
        #job_config.autodetect = True
        #job_config.schema = format_schema(table_schema)
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job = client.load_table_from_json(json_object, table, job_config = job_config)
        print(job.result())
        delete_duplicate()
    except:
        print("Error")
        logging.exception("message")

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.test.shopstar_order_client`))')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId)
        print("Registro: "+str(registro))
        if registro == 300:
            run()
        if registro == 400:
            run()
        if registro == 500:
            run()
        if registro == 600:
            run()
        if registro == 700:
            run()
        if registro == 800:
            run()
        if registro == 900:
            run()
        if registro == 1000:
            run()
        if registro == 1100:
            run()
        if registro == 1200:
            run()
        if registro == 1300:
            run()
        if registro == 1400:
            run()
        if registro == 1500:
            run()
        if registro == 10000:
            run()
        if registro == 15000:
            run()
        if registro == 20000:
            run()
        if registro == 25000:
            run()
        if registro == 30000:
            run()
        if registro == 35000:
            run()
        if registro == 40000:
            run()
        if registro == 45000:
            run()
        if registro == 50000:
            run()
        if registro == 55000:
            run()
        if registro == 60000:
            run()
        if registro == 65000:
            run()
        if registro == 70000:
            run()
        if registro == 75000:
            run()
        if registro == 80000:
            run()
        if registro == 85000:
            run()
        if registro == 90000:
            run()
    run()
           
get_params()