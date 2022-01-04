import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join

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
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        packageAttachment = Fjson["packageAttachment"]
        packages = packageAttachment["packages"]
        
        items = ""
        courier = ""
        invoiceNumber = ""
        invoiceValue = ""
        invoiceUrl = ""
        issuanceDate = ""
        trackingNumber = ""
        invoiceKey = ""
        trackingUrl = ""
        embeddedInvoice = ""
        package_type = ""
        cfop  = ""
        volumes  = ""
        EnableInferItems = ""
        itemIndex = ""
        quantity = ""
        price = ""
        description = ""
        unitMultiplier = ""
        
        for x in packages:
            try:
                items = packages["items"]
            except:
                items = packages[0]
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
            try:
                itemIndex = y["itemIndex"]
                quantity = y["quantity"]
                price = y["price"]
                description = y["description"]
                unitMultiplier = y["unitMultiplier"]
            except:
                itemIndex = y[0]
                quantity = y[1]
                price = y[2]
                description = y[3]
                unitMultiplier = y[4]
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
                'unitMultiplier': unitMultiplier}, index=[0])
            init.df = init.df.append(df1)
    except:
        print("No package")
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_order_package` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_order_package`')
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
    table_schema = [
        {
            "name": "orderId",
            "type": "STRING",
            "mode": "NULLABLE"
    },{
        "name": "courier",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "invoiceNumber",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "invoiceValue",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "invoiceUrl",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "issuanceDate",
        "type": "TIMESTAMP",
        "mode": "NULLABLE"
    },{
        "name": "trackingNumber",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "invoiceKey",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "trackingUrl",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "embeddedInvoice",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "package_type",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "cfop",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "volumes",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "EnableInferItems",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "itemIndex",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "quantity",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "price",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "description",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "unitMultiplier",
        "type": "FLOAT",
        "mode": "NULLABLE"
    }]
    
     
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_order_package'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()
    
    
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order_package(row.orderId)
        print("Registro: "+str(registro))
        if registro == 5:
            run()
        if registro == 100:
            run()
        if registro == 200:
            run()
        if registro == 500:
            run()
        if registro == 1000:
            run()
        if registro == 5000:
            run()
        if registro == 10000:
            run()
    run()
    
get_params()