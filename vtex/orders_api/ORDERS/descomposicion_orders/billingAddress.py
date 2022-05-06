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
    
    '''
    initializing fields and variables
    '''
    productList = []
    df = pd.DataFrame()
    postalCode = None
    city = None
    state = None
    country = None
    street = None
    number = None
    neighborhood = None
    complement = None
    reference = None
    lat = None
    lon = None
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

'''
    querying and saving the data
'''
    
def get_order(id,reg):
    try:
        reg +=1
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        try:
            paymentData = Fjson["paymentData"]
            transactions = paymentData["transactions"]
            for x in transactions:
                payments = x["payments"]
                for x in payments:
                    billingAddress = x["billingAddress"]
                    init.geoCoordinates = billingAddress["geoCoordinates"]
                    init.postalCode = billingAddress["postalCode"]
                    init.city = billingAddress["city"]
                    init.state = billingAddress["state"]
                    init.country = billingAddress["country"]
                    init.street = billingAddress["street"]
                    init.number = billingAddress["number"]
                    init.neighborhood = billingAddress["neighborhood"]
                    init.complement = billingAddress["complement"]
                    init.reference = billingAddress["reference"]
                    init.lot = geoCoordinates[0]
                    init.lan = geoCoordinates[1]
            print("Registro: "+str(reg))
        except:
            print("Registro: "+str(reg))
        df1 = pd.DataFrame({
            'orderId': str(id),
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
            'lat': str(init.lat)}, index=[0])
        init.df = init.df.append(df1)
    except:
        print("empty record")
        print("Error.")
        logging.exception("message")
        

'''
Get parameters using sql query
'''
        
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 1
    for row in rows:
        get_order(row.orderId,registro)
        registro += 1
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_order_billingAddress` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_order_billingAddress`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

def geolocation():
    try:
        print("SQL ST_GEOGPOINT")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_order_billingAddress` AS SELECT *,ST_GEOGPOINT(lon, lat) FROM `shopstar-datalake.test.shopstar_order_billingAddress`')
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
    table_id = 'shopstar_order_billingAddress'
    
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
    geolocation()
    
run()