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
    
    id = None
    email = None
    firstName = None
    lastName = None
    documentType = None
    document = None
    phone = None
    corporateName = None
    tradeName = None
    corporateDocument = None
    stateInscription = None
    corporatePhone = None
    isCorporate = None
    userProfileId = None
    customerClass = None
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
def decrypt_email(email):
    try:
        url = "https://conversationtracker.vtex.com.br/api/pvt/emailMapping?an=mercury&alias="+email+""
        headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
        response = requests.request("GET", url, headers=headers)
        formatoJ = json.loads(response.text)
        return formatoJ["email"]
    except:
        print("No se pudo desencriptar Email: "+str(email))
        
def get_order(id,reg):
    #try:
        reg +=1
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        #try:
        clientProfileData = Fjson["clientProfileData"]
        init.id = clientProfileData["id"]
        init.email = clientProfileData["email"]
        init.firstName = clientProfileData["firstName"]
        init.lastName = clientProfileData["lastName"]
        init.documentType = clientProfileData["documentType"]
        init.document = clientProfileData["document"]
        init.phone = clientProfileData["phone"]
        init.corporateName = clientProfileData["corporateName"]
        init.tradeName = clientProfileData["tradeName"]
        init.corporateDocument = clientProfileData["corporateDocument"]
        init.stateInscription = clientProfileData["stateInscription"]
        init.corporatePhone = clientProfileData["corporatePhone"]
        init.isCorporate = clientProfileData["isCorporate"]
        init.userProfileId = clientProfileData["userProfileId"]
        init.customerClass = clientProfileData["customerClass"]
        client_email = decrypt_email(str(init.email))
        
        df1 = pd.DataFrame({
            'orderId': id,
            'dim_client': init.id,
            'email': client_email,
            'firstName': init.firstName,
            'lastName': init.lastName,
            'documentType': init.documentType,
            'document': init.document,
            'phone': init.phone,
            'corporateName': init.corporateName,
            'tradeName': init.tradeName,
            'corporateDocument': init.corporateDocument,
            'stateInscription': init.stateInscription,
            'corporatePhone': init.corporatePhone,
            'isCorporate': init.isCorporate,
            'userProfileId': init.userProfileId,
            'customerClass': init.customerClass}, index=[0])
        init.df = init.df.append(df1)
        print("Registro: "+str(reg))
        
        
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        get_order(row.orderId,registro)
        registro += 1
        if registro == 15:
            break
        
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
    get_params()
    df = init.df
    print(df)
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_order_client'
    
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
    
run()