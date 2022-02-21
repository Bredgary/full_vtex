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
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def get_order(id,reg):
    try:
        reg +=1
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        paymentData = Fjson["paymentData"]
        transactions = paymentData["transactions"]
        for x in transactions:
            payments = x["payments"]
        for x in payments:
            billingAddress = x["billingAddress"]
            geoCoordinates = billingAddress["geoCoordinates"]
            postalCode = billingAddress["postalCode"]
            city = billingAddress["city"]
            state = billingAddress["state"]
            country = billingAddress["country"]
            street = billingAddress["street"]
            number = billingAddress["number"]
            neighborhood = billingAddress["neighborhood"]
            complement = billingAddress["complement"]
            reference = billingAddress["reference"]
            lot = geoCoordinates[0]
            lan = geoCoordinates[1]
            print("Registro: "+str(reg))
            df1 = pd.DataFrame({
                'orderId': id,
                'postalCode': postalCode,
                'city': city,
                'state': state,
                'country': country,
                'street': street,
                'number': number,
                'neighborhood': neighborhood,
                'complement': complement,
                'reference': reference,
                'lon': lon,
                'lat': lat}, index=[0])
            init.df = init.df.append(df1)
        if df1.empty:
            df1 = pd.DataFrame({
            'orderId': id}, index=[0])
            init.df = init.df.append(df1)
    except:
        df1 = pd.DataFrame({
            'orderId': id}, index=[0])
        init.df = init.df.append(df1)
        
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_order_billingAddress` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_order_billingAddress`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

def geolocation():
    try:
        print("SQL ST_GEOGPOINT")
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_order_billingAddress` AS SELECT *,ST_GEOGPOINT(lon, lat) FROM `shopstar-datalake.staging_zone.shopstar_order_billingAddress`')
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
            "name": "lat",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "neighborhood",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "number",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "street",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "state",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "reference",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "country",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "lon",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "complement",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "orderId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "city",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "postalCode",
            "type": "STRING",
            "mode": "NULLABLE"
        }] 
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_order_billingAddress'
        
        client  = bigquery.Client(project = project_id)
        dataset  = client.dataset(dataset_id)
        table = dataset.table(table_id)
        
        if df.empty:
            print('DataFrame is empty!')
        else:
            try:
                client  = bigquery.Client(project = project_id)
                dataset  = client.dataset(dataset_id)
                table = dataset.table(table_id)
                job_config = bigquery.LoadJobConfig()
                job_config.schema = format_schema(table_schema)
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                job = client.load_table_from_json(json_object, table, job_config = job_config)
                print(job.result())
                delete_duplicate()
            except:
                client  = bigquery.Client(project = project_id)
                dataset  = client.dataset(dataset_id)
                table = dataset.table(table_id)
                job_config = bigquery.LoadJobConfig()
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                job = client.load_table_from_json(json_object, table, job_config = job_config)
                print(job.result())
                delete_duplicate()
    except:
        print("Error.")
        logging.exception("message")
    
    
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_order_billingAddress`))')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId,registro)
        print("Registro: "+str(registro))
        if registro == 10:
            run()
        if registro == 20:
            run()
        if registro == 30:
            run()
        if registro == 40:
            run()
        if registro == 50:
            run()
    run()

get_params() 