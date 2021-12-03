#!/usr/bin/python
# -*- coding: latin-1 -*-
import pandas as pd
import numpy as np
import requests
import json
import os
import re
import datetime
from datetime import date
from datetime import timedelta
from os import system
from google.cloud import bigquery
import logging

class init:
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    before_yesterday = today - datetime.timedelta(days=2)
    ordenes = {}
    df = pd.DataFrame()

class date:
    import pandas as pd
    import numpy as np
    from google.cloud import bigquery
    import os, json
    from datetime import datetime
    import requests
    from datetime import datetime, timezone
    
    naive_dt = datetime.now()
    aware_dt = naive_dt.astimezone()
    # correct, ISO-8601 (but not UTC)
    aware_dt.isoformat(timespec="seconds")
    # lets get the time in UTC
    utc_dt = aware_dt.astimezone(timezone.utc)
    # correct, ISO-8601 and UTC (but not in UTC format)
    date_str = utc_dt.isoformat(timespec='milliseconds')
    date = date_str.replace("+00:00", "Z")
    now = datetime.now()
    format = now.strftime('%Y-%m-%d')
    


class Init:
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    ordenes = {}
    df = pd.DataFrame()
    registro = 0

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def get_order_list(page,reg):
    url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders?page="+str(page)+""
    querystring = {"f_creationDate":"creationDate:[2020-01-01T02:00:00.000Z TO 2020-01-02T01:59:59.999Z]","f_hasInputInvoice":"false"}
    headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    FJson = json.loads(response.text)
    if FJson["list"]:
        for x in FJson:
            print(FJson["orderId"])
            df1 = pd.DataFrame({
                'orderId': x["orderId"],
                'creationDate': x["creationDate"],
                'clientName': x["clientName"],
                'totalValue': x["totalValue"],
                'paymentNames': x["paymentNames"],
                'status': x["status"],
                'statusDescription': x["statusDescription"],
                'marketPlaceOrderId': x["marketPlaceOrderId"],
                'sequence': x["sequence"],
                'salesChannel': x["salesChannel"],
                'affiliateId': x["affiliateId"],
                'origin': x["origin"],
                'workflowInErrorSta1te': x["workflowInErrorState"],
                'workflowInRetry': x["workflowInRetry"],
                'lastMessageUnread': x["lastMessageUnread"],
                'ShippingEstimatedDate': x["ShippingEstimatedDate"],
                'ShippingEstimatedDateMax': x["ShippingEstimatedDateMax"],
                'ShippingEstimatedDateMin': x["ShippingEstimatedDateMin"],
                'orderIsComplete': x["orderIsComplete"],
                'listId': x["listId"],
                'listType': x["listType"],
                'authorizedDate': x["authorizedDate"],
                'callCenterOperatorName': x["callCenterOperatorName"],
                'totalItems': x["totalItems"],
                'currencyCode': x["currencyCode"]}, index=[0])
        init.df = init.df.append(df1)
        print("Registro: "+str(reg))
    return FJson


def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_list_order` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")


def run():
    for x in range(30):
        init.registro += 1
        get_order_list(init.registro,reg)
        
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    table_schema = {
            "name": "orderId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "creationDate",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "clientName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "totalValue",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "paymentNames",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "status",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "statusDescription",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "marketPlaceOrderId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "sequence",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "salesChannel",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "affiliateId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "origin",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "workflowInErrorState",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "workflowInRetry",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "lastMessageUnread",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ShippingEstimatedDate",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ShippingEstimatedDateMax",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ShippingEstimatedDateMin",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "orderIsComplete",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "listId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "listType",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "authorizedDate",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "callCenterOperatorName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "totalItems",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "currencyCode",
            "type": "STRING",
            "mode": "NULLABLE"}
    
    project_id = '999847639598'
    dataset_id = 'staging_zone'
    table_id = 'shopstar_vtex_list_order_test'
    table_temp = 'order_write'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    tableO = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = format_schema(table_schema)
    job = client.load_table_from_json(json_object, tableO, job_config = job_config)
    print(job.result())
    
    tableT = dataset.table(table_temp)
    job_config_temp = bigquery.LoadJobConfig()
    job_config_temp.write_disposition = "WRITE_TRUNCATE"
    job_config_temp.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config_temp.schema = format_schema(table_schema)
    job = client.load_table_from_json(json_object, tableT, job_config = job_config_temp)
    print(job.result())

run()



