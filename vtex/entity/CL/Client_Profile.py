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
  
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
def get_order(id,email,reg):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/checkout/pub/profiles"
        querystring = {"email":""+str(email)+""}
        response = requests.request("GET", url, headers=init.headers, params=querystring)
        Fjson = json.loads(response.text)
        try:
            init.userProfileId = Fjson["userProfileId"]
            init.profileProvider = Fjson["profileProvider"]
        except:
            print("Vacio")
        
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
            print("Vacio")
        
        try:
            init.userProfile = Fjson["userProfile"]
            init.email = init.userProfile["email"]
            init.firstName = init.userProfile["firstName"]
            init.lastName = init.userProfile["lastName"]
            init.document = init.userProfile["document"]
            init.documentType = init.userProfile["documentType"]
            init.phone = init.userProfile["phone"]
            init.corporateName = init.userProfile["corporateName"]
            init.tradeName = init.userProfile["tradeName"]
            init.corporateDocument = init.userProfile["corporateDocument"]
            init.stateInscription = init.userProfile["stateInscription"]
            init.corporatePhone = init.userProfile["corporatePhone"]
            init.isCorporate = init.userProfile["isCorporate"]
            init.profileCompleteOnLoading = init.userProfile["profileCompleteOnLoading"]
            init.profileErrorOnLoading = init.userProfile["profileErrorOnLoading"]
            init.isComplete = Fjson["profileProvider"]
        except:
            print("Vacio")
        
        
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
            'isComplete': str(init.isComplete)}, index=[0])
        init.df = init.df.append(df1)
        print("Registro: "+str(reg))
    except:
        print("Registro: "+str(reg))
        
        
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


def run():
    get_params()
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
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()
    
run()
