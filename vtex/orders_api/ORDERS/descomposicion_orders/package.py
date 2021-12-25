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


def get_order(id,reg):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        packageAttachment = Fjson["packageAttachment"]
        packages = packageAttachment["packages"]
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
            try:
                courierStatus = x["courierStatus"]
                for j in courierStatus:
                    status = j[0]
                    finished = j[1]
                    deliveredDate = j[2]
            except:
                courierStatus =""
                status =""
                finished =""
                deliveredDate =""
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
                    'status': status,
                    'finished': finished,
                    'deliveredDate': deliveredDate,
                    'itemIndex': itemIndex,
                    'quantity': quantity,
                    'price': price,
                    'description': description,
                    'unitMultiplier': init.unitMultiplier}, index=[0])
                init.df = init.df.append(df1)
                print("Registro: "+str(reg))
    except:
        print("No packages "+str(reg))    

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order("1040711467154-01",registro)
        if registro == 1:
            run()
    run()
        
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
        "type": "DATE",
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
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "type",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "status",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "finished",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "deliveredDate",
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
        "type": "INTEGER",
        "mode": "NULLABLE"
    }]
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_order_package'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    
    job_config = bigquery.LoadJobConfig()
    job_config.schema = format_schema(table_schema)
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    
get_params()