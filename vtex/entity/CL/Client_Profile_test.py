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
  
  userProfileId = None
  profileProvider = None
  availableAddresses = None
  addressType = None
  receiverName = None
  addressId = None
  isDisposable = None
  postalCode = None
  city = None
  state = None
  country = None
  street = None
  number = None
  neighborhood = None
  complement = None
  reference = None
  geoCoordinates = None
  lon = None
  lat = None
  
  userProfile = None
  email = None
  firstName = None
  lastName = None
  document = None
  documentType = None
  phone = None
  corporateName = None
  tradeName = None
  corporateDocument = None
  stateInscription = None
  corporatePhone = None
  isCorporate = None
  profileCompleteOnLoading = None
  profileErrorOnLoading = None
  isComplete = None
  accountId = None
  paymentSystem = None
  paymentSystemName = None
  cardNumber = None
  bin = None
  availableAddresses_0 = None
  availableAddresses_1 = None
  expirationDate = None
  isExpired = None
  
  
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
def get_order(id,email,reg):
    try:
        print(email)
        url = "https://mercury.vtexcommercestable.com.br/api/checkout/pub/profiles"
        querystring = {"email":""+str(email)+""}
        response = requests.request("GET", url, headers=init.headers, params=querystring)
        Fjson = json.loads(response.text)
        try:
            init.userProfileId = Fjson["userProfileId"]
            init.profileProvider = Fjson["profileProvider"]
            init.isComplete = Fjson["isComplete"]
        except:
            userProfileId = "Vacio"
        try:
            availableAccounts = Fjson["availableAccounts"]
            for x in availableAccounts:
                init.accountId = x["accountId"]
                init.paymentSystem = x["paymentSystem"]
                init.paymentSystemName = x["paymentSystemName"]
                init.cardNumber = x["cardNumber"]
                init.bin = x["bin"]
                try:
                    availableAddresses = x["availableAddresses"]
                    init.availableAddresses_0 = availableAddresses[0]
                    init.availableAddresses_1 = availableAddresses[1]
                except:
                    print("Ramificacion no existe")
                init.expirationDate = x["expirationDate"]
                init.isExpired = x["isExpired"]
        except:
            print("availableAddresses Vacio")
        
        try:
            init.availableAddresses = Fjson["availableAddresses"]
            for x in init.availableAddresses:
                init.addressType = x["addressType"]
                init.receiverName = x["receiverName"]
                init.addressId = x["addressId"]
                init.isDisposable = x["isDisposable"]
                init.postalCode = x["postalCode"]
                init.city = x["city"]
                init.state = x["state"]
                init.country = x["country"]
                init.street = x["street"]
                init.number = x["number"]
                init.neighborhood = x["neighborhood"]
                init.complement = x["complement"]
                init.reference = x["reference"]
                init.geoCoordinates = x["geoCoordinates"]
                if geoCoordinates:
                    init.lon = geoCoordinates[0]
                    init.lat = geoCoordinates[1]
        except:
            print("availableAddresses Vacio")
        
        try:
            userProfile = Fjson["userProfile"]
            init.email = userProfile["email"]
            init.firstName = userProfile["firstName"]
            init.lastName = userProfile["lastName"]
            init.document = userProfile["document"]
            init.documentType = userProfile["documentType"]
            init.phone = userProfile["phone"]
            init.corporateName = userProfile["corporateName"]
            init.tradeName = userProfile["tradeName"]
            init.corporateDocument = userProfile["corporateDocument"]
            init.stateInscription = userProfile["stateInscription"]
            init.corporatePhone = userProfile["corporatePhone"]
            init.isCorporate = userProfile["isCorporate"]
            init.profileCompleteOnLoading = userProfile["profileCompleteOnLoading"]
            init.profileErrorOnLoading = userProfile["profileErrorOnLoading"]
        except:
            print("client profile")
        
        
        df1 = pd.DataFrame({
            'customer_id': str(id),
            'userProfileId': str(init.userProfileId),
            'profileProvider': str(init.profileProvider),
            'addressType': str(init.addressType),
            'receiverName': str(init.receiverName),
            'addressId': str(init.addressId),
            'isDisposable': str(init.isDisposable),
            'postalCode': str(init.postalCode),
            'city': str(init.city),
            'state': str(init.state),
            'country': str(init.country),
            'street': str(init.street),
            'number': str(init.number),
            'neighborhood': str(init.neighborhood),
            'complement': str(init.complement),
            'reference': str(init.reference),
            'lon': str(init.lon),
            'lat': str(init.lat),
            'userProfile': str(init.userProfile),
            'email': str(init.email),
            'firstName': str(init.firstName),
            'lastName': str(init.lastName),
            'document': str(init.document),
            'documentType': str(init.documentType),
            'phone': str(init.phone),
            'corporateName': str(init.corporateName),
            'tradeName': str(init.tradeName),
            'corporateDocument': str(init.corporateDocument),
            'stateInscription': str(init.stateInscription),
            'corporatePhone': str(init.corporatePhone),
            'isCorporate': str(init.isCorporate),
            'profileCompleteOnLoading': str(init.profileCompleteOnLoading),
            'profileErrorOnLoading': str(init.profileErrorOnLoading),
            'accountId': str(init.accountId),
            'paymentSystem': str(init.paymentSystem),
            'paymentSystemName': str(init.paymentSystemName),
            'cardNumber': str(init.cardNumber),
            'bin': str(init.bin),
            'availableAddresses_0': str(init.availableAddresses_0),
            'availableAddresses_1': str(init.availableAddresses_1),
            'expirationDate': str(init.expirationDate),
            'isExpired': str(init.isExpired),
            'isComplete': str(init.isComplete)}, index=[0])
        init.df = init.df.append(df1)
        print("Registro: "+str(reg))
    except:
        print("No data profile "+str(reg))
        
        
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT id,email FROM `shopstar-datalake.staging_zone.shopstar_vtex_client`')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.id,row.email,registro)
        if registro == 5000:
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
    run()
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_vtex_client_profile_test` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_vtex_client_profile_test`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")


def run():
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_vtex_client_profile_test'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()
    
get_params()
