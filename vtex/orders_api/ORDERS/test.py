import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join
from pip._vendor.pyparsing import empty
import logging

class init:
    df = pd.DataFrame()
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}


def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema


def get_order_package(id):
    try:
        df1 = pd.DataFrame()
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        packageAttachment = Fjson["packageAttachment"]
        packages = packageAttachment["packages"]
        lastChange = Fjson["lastChange"]
        for x in packages:
            items = x["items"]
            courier = x["courier"]
            invoiceNumber = x["invoiceNumber"]
            invoiceValue = x["invoiceValue"]
            invoiceUrl = x["invoiceUrl"]
            issuanceDate = x["issuanceDate"]
            trackingNumber = x["trackingNumber"]
            invoiceKey = x["invoiceKey"]
            trackingUrl = x["trackingUrl"]
            embeddedInvoice = x["embeddedInvoice"]
            package_type = x["type"]
            cfop = x["cfop"]
            volumes = x["volumes"]
            EnableInferItems = x["EnableInferItems"]
            for y in items:
                itemIndex = y["itemIndex"]
                quantity = y["quantity"]
                price = y["price"]
                description = y["description"]
                unitMultiplier = y["unitMultiplier"]
                df1 = pd.DataFrame({
                    'orderId': id,
                    'courier': courier,
                    'invoiceNumber': invoiceNumber,
                    'invoiceValue': invoiceValue,
                    'invoiceUrl': invoiceUrl,
                    'issuanceDate': issuanceDate,
                    'trackingNumber': trackingNumber,
                    'invoiceKey': invoiceKey,
                    'trackingUrl': trackingUrl,
                    'embeddedInvoice': embeddedInvoice,
                    'package_type': package_type,
                    "cfop":cfop,
                    "volumes":volumes,
                    "EnableInferItems":EnableInferItems,
                    'itemIndex': itemIndex,
                    'quantity': quantity,
                    'price': price,
                    'description': description,
                    'unitMultiplier': unitMultiplier,
                    'lastChange': lastChange}, index=[0])
                init.df = init.df.append(df1)
        if df1.empty:
            df1 = pd.DataFrame({
                'orderId': id}, index=[0])
            init.df = init.df.append(df1)
    except:
        print("Error.")
        logging.exception("message")
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_order_package` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_order_package`')
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
            "name": "description",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "itemIndex",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "price",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "EnableInferItems",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "cfop",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "embeddedInvoice",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "invoiceNumber",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "invoiceKey",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "issuanceDate",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },{
            "name": "orderId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "invoiceValue",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "unitMultiplier",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "trackingNumber",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "quantity",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "courier",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "volumes",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "package_type",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "trackingUrl",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "invoiceUrl",
            "type": "STRING",
            "mode": "NULLABLE"
        }] 
         
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_order_package'
        
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
    QUERY = ('SELECT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_order_package`))')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order_package(row.orderId)
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
        if registro == 60:
            run()
        if registro == 70:
            run()
        if registro == 80:
            run()
        if registro == 90:
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
        if registro == 130000:
            run()
    run()
    
get_params()