import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from os.path import join

class init:
  df = pd.DataFrame()

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def get_order(id):
    try:
        headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=headers)
        Fjson = json.loads(response.text)
        
        shippingData = Fjson["shippingData"]
        shipping_selectedAddresses= shippingData["selectedAddresses"]
        
        for x in shipping_selectedAddresses:
            addressId = x["addressId"]
            addressType = x["addressType"]
            receiverName = x["receiverName"]
            street = x["street"]
            number = x["number"]
            complement = x["complement"]
            neighborhood = x["neighborhood"]
            postalCode = x["postalCode"]
            city = x["city"]
            state = x["state"]
            country = x["country"]
            reference = x["reference"]
            try:
                geoCoordinates  = x["geoCoordinates"]
                geoCoordinates_LAT = geoCoordinates[0]
                geoCoordinates_LON = geoCoordinates[1]
            except:
                geoCoordinates_LAT = 0
                geoCoordinates_LON = 0
                print("No geoCoordinates")
            
            df1 = pd.DataFrame({
                'orderId': id,
                'addressId': addressId,
                'addressType': addressType,
                'receiverName': receiverName,
                'street': street,
                'number': number,
                'complement': complement,
                'neighborhood': neighborhood,
                'postalCode': postalCode,
                'city': city,
                'state': state,
                'country': country,
                'reference': reference,
                'geoCoordinates_LAT': geoCoordinates_LAT,
                'geoCoordinates_LON': geoCoordinates_LON}, index=[0])
        init.df = init.df.append(df1)
    except:
        print(id)
        print("Vacio")

def delete_duplicate():
  try:
    print("Eliminando duplicados")
    client = bigquery.Client()
    QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shippingData_selectedAddresses` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shippingData_selectedAddresses`')
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
            "name": "geoCoordinates_LON",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "geoCoordinates_LAT",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "neighborhood",
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
            "name": "addressType",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "state",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "complement",
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
            "name": "orderId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "city",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "receiverName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "postalCode",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "addressId",
            "type": "STRING",
            "mode": "NULLABLE"
        }] 
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shippingData_selectedAddresses'
        
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
    QUERY = ('SELECT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.staging_zone.shippingData_selectedAddresses`))')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId)
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

