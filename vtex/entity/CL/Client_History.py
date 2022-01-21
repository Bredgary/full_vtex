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
  
def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def get_order(email,reg):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/dataentities/CL/search"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/vnd.vtex.ds.v10+json",
            "REST-Range": "resources=0-50",
            "X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
            "X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
        
        querystring = {"_fields":"beneficio,beneficio2,crearGiftcard,profilePicture,proteccionDatos,terminosCondiciones,terminosPago,tradeName,rclastcart,rclastsession,rclastsessiondate,homePhone,phone,stateRegistration,email,userId,firstName,lastName,document,localeDefault,attach,approved,birthDate,businessPhone,corporateDocument,corporateName,documentType,gender,customerClass,priceTables,id,accountId,accountName,dataEntityId,createdBy,createdIn,updatedBy,updatedIn,lastInteractionBy,lastInteractionIn","_where":"email = "+str(email)+""}
        response = requests.request("GET", url, headers=headers, params=querystring)
        Fjson = json.loads(response.text)
        for x in Fjson:
            beneficio = x["beneficio"]
            beneficio2 = x["beneficio2"]
            crearGiftcard = x["crearGiftcard"]
            profilePicture = x["profilePicture"]
            proteccionDatos = x["proteccionDatos"]
            terminosCondiciones = x["terminosCondiciones"]
            terminosPago = x["terminosPago"]
            tradeName = x["tradeName"]
            rclastcart = x["rclastcart"]
            rclastsession = x["rclastsession"]
            rclastsessiondate = x["rclastsessiondate"]
            homePhone = None
            phone = x["phone"]
            stateRegistration = x["stateRegistration"]
            email = x["email"]
            userId = x["userId"]
            firstName = x["firstName"]
            lastName = x["lastName"]
            document = x["document"]
            localeDefault = x["localeDefault"]
            attach = x["attach"]
            approved = x["approved"]
            birthDate = x["birthDate"]
            businessPhone = x["businessPhone"]
            corporateDocument = x["corporateDocument"]
            corporateName = x["corporateName"]
            documentType = x["documentType"]
            gender = x["gender"]
            customerClass = x["customerClass"]
            priceTables = x["priceTables"]
            id = x["id"]
            accountId = x["accountId"]
            accountName = x["accountName"]
            dataEntityId = x["dataEntityId"]
            createdBy = x["createdBy"]
            createdIn = x["createdIn"]
            updatedBy = x["updatedBy"]
            updatedIn = x["updatedIn"]
            lastInteractionBy = x["lastInteractionBy"]
            lastInteractionIn = x["lastInteractionIn"]
            
            df1 = pd.DataFrame({
                'beneficio':beneficio,
                'beneficio2':beneficio2,
                'crearGiftcard':crearGiftcard,
                'profilePicture':profilePicture,
                'proteccionDatos':proteccionDatos,
                'terminosCondiciones':terminosCondiciones,
                'terminosPago':terminosPago,
                'tradeName':tradeName,
                'rclastcart':rclastcart,
                'rclastsession':rclastsession,
                'rclastsessiondate':rclastsessiondate,
                'homePhone':homePhone,
                'phone':phone,
                'stateRegistration':stateRegistration,
                'email':email,
                'userId':userId,
                'firstName':firstName,
                'lastName':lastName,
                'document':document,
                'localeDefault':localeDefault,
                'attach':attach,
                'approved':approved,
                'birthDate':birthDate,
                'businessPhone':businessPhone,
                'corporateDocument':corporateDocument,
                'corporateName':corporateName,
                'documentType':documentType,
                'gender':gender,
                'customerClass':customerClass,
                'priceTables':priceTables,
                'id':id,
                'accountId':accountId,
                'accountName':accountName,
                'dataEntityId':dataEntityId,
                'createdBy':createdBy,
                'createdIn':createdIn,
                'updatedBy':updatedBy,
                'updatedIn':updatedIn,
                'lastInteractionBy':lastInteractionBy,
                'lastInteractionIn':lastInteractionIn}, index=[0])
            init.df = init.df.append(df1)
            print(init.df)
        print("Registro: "+str(reg))
    except:
        print("No data "+str(reg))

def run():
    try:
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)    
        
        table_schema = [
        {
            "name": "lastInteractionBy",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "updatedBy",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "createdIn",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },{
            "name": "createdBy",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "dataEntityId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "priceTables",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "customerClass",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "gender",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "documentType",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "corporateName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "corporateDocument",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "businessPhone",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "accountName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "lastName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "homePhone",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "email",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "firstName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "lastInteractionIn",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },{
            "name": "userId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "stateRegistration",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "tradeName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "updatedIn",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },{
            "name": "terminosCondiciones",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "birthDate",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },{
            "name": "attach",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "rclastsessiondate",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },{
            "name": "rclastsession",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "profilePicture",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "approved",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "phone",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "beneficio2",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "proteccionDatos",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "document",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "id",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "crearGiftcard",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "terminosPago",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "rclastcart",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "accountId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "localeDefault",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "beneficio",
            "type": "STRING",
            "mode": "NULLABLE"
        }]
           
        
        project_id = '999847639598'
        dataset_id = 'test'
        table_id = 'shopstar_vtex_client_history_'
        
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
            job_config.schema = format_schema(table_schema)
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job = client.load_table_from_json(json_object, table, job_config = job_config)
            print(job.result())
    except:
        print("Error.")
        logging.exception("message")
        
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT email  FROM `shopstar-datalake.cons_zone.dm_customer`WHERE (email NOT IN (SELECT email FROM `shopstar-datalake.test.shopstar_vtex_client_history_`))')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.email,registro)
        print(row.email)
        if registro == 1:
            run()
        if registro == 50:
            run()
        if registro == 100:
            run()
        if registro == 150:
            run()
        if registro == 200:
            run()
        if registro == 200:
            run()
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
        if registro == 2000:
            run()
        if registro == 2500:
            run()
        if registro == 3000:
            run()
        if registro == 3500:
            run()
        if registro == 4000:
            run()
        if registro == 4500:
            run()
        if registro == 5000:
            run()
        if registro == 5500:
            run()
        if registro == 6000:
            run()
        if registro == 6500:
            run()
        if registro == 7000:
            run()
        if registro == 7500:
            run()
        if registro == 8000:
            run()
        if registro == 8500:
            run()
        if registro == 9000:
            run()
        if registro == 9500:
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
        if registro == 95000:
            run()
        if registro == 100000:
            run()
        if registro == 105000:
            run()
        if registro == 110000:
            run()
        if registro == 115000:
            run()
        if registro == 120000:
            run()
        if registro == 125000:
            run()
    run()
get_params()
