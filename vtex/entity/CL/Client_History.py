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
  

def get_order(email,reg):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/dataentities/CL/search?_where=email=*"+str(email)+""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/vnd.vtex.ds.v10+json",
            "REST-Range": "resources=0-1000",
            "X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
            "X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
        
        querystring = {"_fields":"beneficio,beneficio2,crearGiftcard,profilePicture,proteccionDatos,terminosCondiciones,terminosPago,tradeName,rclastcart,rclastsession,rclastsessiondate,homePhone,phone,stateRegistration,email,userId,firstName,lastName,document,localeDefault,attach,approved,birthDate,businessPhone,corporateDocument,corporateName,documentType,gender,customerClass,priceTables,id,accountId,accountName,dataEntityId,createdBy,createdIn,updatedBy,updatedIn,lastInteractionBy,lastInteractionIn"}
        response = requests.request("GET", url, headers=headers, params=querystring)
        Fjson = json.loads(response.text)
        return Fjson
        print("Registro "+str(reg))
    except:
        print("No data "+str(reg))

def run(email,reg):
    try:
        df = pd.DataFrame(get_order(email,reg),columns=['beneficio','beneficio2','crearGiftcard','profilePicture','proteccionDatos','terminosCondiciones','terminosPago','tradeName','rclastcart','rclastsession','rclastsessiondate','homePhone','phone','stateRegistration','email','userId','firstName','lastName','document','localeDefault','attach','approved','birthDate','businessPhone','corporateDocument','corporateName','documentType','gender','customerClass','priceTables','id','accountId','accountName','dataEntityId','createdBy','createdIn','updatedBy','updatedIn','lastInteractionBy','lastInteractionIn'])
        df.reset_index(drop=True, inplace=True)
        
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        project_id = '999847639598'
        dataset_id = 'test'
        table_id = 'shopstar_vtex_client_history'
        
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
            job_config.write_disposition = "WRITE_TRUNCATE"
            job_config.autodetect = True
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job = client.load_table_from_json(json_object, table, job_config = job_config)
            print(job.result())
    except:
        print("Error.")
        logging.exception("message")
        
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT email FROM `shopstar-datalake.cons_zone.dm_customer`')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        run(row.email,registro)
        
  
get_params()
