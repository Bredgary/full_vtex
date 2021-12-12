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
    orderId = None
    document = None
    beneficio = None
    beneficio2 = None
    crearGiftcard = None
    profilePicture = None
    proteccionDatos = None
    terminosCondiciones = None
    terminosPago = None
    tradeName = None
    rclastcart = None
    rclastsession = None
    rclastsessiondate = None
    homePhone = None
    phone = None
    stateRegistration = None
    email = None
    userId = None
    firstName = None
    lastName = None
    document = None
    localeDefault = None
    attach  = None
    approved = None
    birthDate = None
    businessPhone = None
    corporateDocument = None
    corporateName  = None
    documentType = None
    gender = None
    customerClass = None
    priceTables = None
    id = None
    accountId = None
    accountName = None
    dataEntityId = None
    createdBy = None
    createdIn = None
    updatedBy = None
    updatedIn = None
    lastInteractionBy = None
    lastInteractionIn = None
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
def get_order(id,reg):
    url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
    response = requests.request("GET", url, headers=init.headers)
    Fjson = json.loads(response.text)
    clientProfileData = Fjson["clientProfileData"]
    init.document = clientProfileData["document"]
    init.orderId = id
    print("Registro: "+str(reg))
    cl_client(init.orderId,init.document)
        
def cl_client(order,document):
    url = "https://mercury.vtexcommercestable.com.br/api/dataentities/CL/search"
    querystring = {"_fields":"beneficio,beneficio2,crearGiftcard,profilePicture,proteccionDatos,terminosCondiciones,terminosPago,tradeName,rclastcart,rclastsession,rclastsessiondate,homePhone,phone,stateRegistration,email,userId,firstName,lastName,document,localeDefault,attach,approved,birthDate,businessPhone,corporateDocument,corporateName,documentType,gender,customerClass,priceTables,id,accountId,accountName,dataEntityId,createdBy,createdIn,updatedBy,updatedIn,lastInteractionBy,lastInteractionIn","_where":"document="+document+""}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.vtex.ds.v10+json",
        "REST-Range": "resources=0-1",
        "X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
        "X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    Fjson = json.loads(response.text)
    for x in Fjson:
        init.beneficio = x["beneficio"]
        init.beneficio2 = x["beneficio2"]
        init.crearGiftcard = x["crearGiftcard"]
        init.profilePicture = x["profilePicture"]
        init.proteccionDatos = x["proteccionDatos"]
        init.terminosCondiciones = x["terminosCondiciones"]
        init.terminosPago = x["terminosPago"]
        init.tradeName = x["tradeName"]
        init.rclastcart = x["rclastcart"]
        init.rclastsession = x["rclastsession"]
        init.rclastsessiondate = x["rclastsessiondate"]
        init.homePhone = x["homePhone"]
        init.phone = x["phone"]
        init.stateRegistration = x["stateRegistration"]
        init.email = x["email"]
        init.userId = x["userId"]
        init.firstName = x["firstName"]
        init.lastName = x["lastName"]
        init.document = x["document"]
        init.localeDefault = x["localeDefault"]
        init.attach = x["attach"]
        init.approved = x["approved"]
        init.birthDate = x["birthDate"]
        init.businessPhone = x["businessPhone"]
        init.corporateDocument = x["corporateDocument"]
        init.corporateName = x["corporateName"]
        init.documentType = x["documentType"]
        init.gender = x["gender"]
        init.customerClass = x["customerClass"]
        init.priceTables = x["priceTables"]
        init.id = x["id"]
        init.accountId = x["accountId"]
        init.accountName = x["accountName"]
        init.dataEntityId = x["dataEntityId"]
        init.createdBy = x["createdBy"]
        init.createdIn = x["createdIn"]
        init.updatedBy = x["updatedBy"]
        init.updatedIn = x["updatedIn"]
        init.lastInteractionBy = x["lastInteractionBy"]
        init.lastInteractionIn = x["lastInteractionIn"]
    
    
    df1 = pd.DataFrame({
        'orderId': order,
        'beneficio': init.beneficio,
        'beneficio2': init.beneficio2,
        'crearGiftcard': init.crearGiftcard,
        'profilePicture': init.profilePicture,
        'proteccionDatos': init.proteccionDatos,
        'terminosCondiciones': init.terminosCondiciones,
        'terminosPago': init.terminosPago,
        'tradeName': init.tradeName,
        'rclastcart': init.rclastcart,
        'rclastsession': init.rclastsession,
        'rclastsessiondate': init.rclastsessiondate,
        'homePhone': init.homePhone,
        'phone': init.phone,
        'stateRegistration': init.stateRegistration,
        'email': init.email,
        'userId': init.userId,
        'firstName': init.firstName,
        'lastName': init.lastName,
        'document': init.document,
        'attach': init.attach,
        'approved': init.approved,
        'birthDate': init.birthDate,
        'businessPhone': init.businessPhone,
        'corporateDocument': init.corporateDocument,
        'gender': init.gender,
        'customerClass': init.customerClass,
        'priceTables': init.priceTables,
        'id': init.id,
        'accountId': init.accountId,
        'accountName': init.accountName,
        'dataEntityId': init.dataEntityId,
        'createdBy': init.createdBy,
        'createdBy': init.createdBy,
        'createdIn': init.createdIn,
        'updatedBy': init.updatedBy,
        'lastInteractionBy': init.lastInteractionBy,
        'lastInteractionIn': init.lastInteractionIn}, index=[0])
    init.df = init.df.append(df1)
        
        
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId,registro)
        if registro == 15:
            break
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_order_client_full` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_order_client_full`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")


def run():
    get_params()
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_order_client_full'
    
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