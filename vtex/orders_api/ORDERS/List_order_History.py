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
    salir = 0
    df = pd.DataFrame()
    registro = 0
    reg = 0
    num_from = "05"
    num_to ="05"
    mount = "02"
    '''
    28
    '''
    headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    hora_0 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T01:00:00.000Z TO 2021-"+mount+"-"+num_to+"T01:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_1 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T02:00:00.000Z TO 2021-"+mount+"-"+num_to+"T02:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_2 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T03:00:00.000Z TO 2021-"+mount+"-"+num_to+"T03:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_3 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T04:00:00.000Z TO 2021-"+mount+"-"+num_to+"T04:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_4 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T05:00:00.000Z TO 2021-"+mount+"-"+num_to+"T05:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_5 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T06:00:00.000Z TO 2021-"+mount+"-"+num_to+"T06:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_6 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T07:00:00.000Z TO 2021-"+mount+"-"+num_to+"T07:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_7 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T08:00:00.000Z TO 2021-"+mount+"-"+num_to+"T08:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_8 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T09:00:00.000Z TO 2021-"+mount+"-"+num_to+"T19:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_9 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T10:00:00.000Z TO 2021-"+mount+"-"+num_to+"T10:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_10 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T11:00:00.000Z TO 2021-"+mount+"-"+num_to+"T11:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_11 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T12:00:00.000Z TO 2021-"+mount+"-"+num_to+"T12:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_12 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T13:00:00.000Z TO 2021-"+mount+"-"+num_to+"T13:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_13 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T14:00:00.000Z TO 2021-"+mount+"-"+num_to+"T14:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_14 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T15:00:00.000Z TO 2021-"+mount+"-"+num_to+"T15:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_15 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T16:00:00.000Z TO 2021-"+mount+"-"+num_to+"T16:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_16 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T17:00:00.000Z TO 2021-"+mount+"-"+num_to+"T17:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_17 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T18:00:00.000Z TO 2021-"+mount+"-"+num_to+"T18:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_18 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T19:00:00.000Z TO 2021-"+mount+"-"+num_to+"T19:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_19 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T20:00:00.000Z TO 2021-"+mount+"-"+num_to+"T20:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_20 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T21:00:00.000Z TO 2021-"+mount+"-"+num_to+"T21:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_21 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T22:00:00.000Z TO 2021-"+mount+"-"+num_to+"T22:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_22 = {"f_creationDate":"creationDate:[2021-"+mount+"-"+num_from+"T23:00:00.000Z TO 2021-"+mount+"-"+num_to+"T23:59:59.999Z]","f_hasInputInvoice":"false"}

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
    
def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema


def get_order_list(page,hora):
    url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders?page="+str(page)+""
    response = requests.request("GET", url, headers=init.headers, params=hora)
    FJTemp = json.loads(response.text)
    FJson = FJTemp["list"]
    if FJson:
        for x in FJson:
            init.reg +=1
            df1 = pd.DataFrame({
                'orderId': str(x["orderId"]),
                'creationDate': str(x["creationDate"]),
                'clientName': str(x["clientName"]),
                'totalValue': str(x["totalValue"]),
                'paymentNames': str(x["paymentNames"]),
                'status': str(x["status"]),
                'statusDescription': str(x["statusDescription"]),
                'marketPlaceOrderId': str(x["marketPlaceOrderId"]),
                'sequence': str(x["sequence"]),
                'salesChannel': str(x["salesChannel"]),
                'affiliateId': str(x["affiliateId"]),
                'origin': str(x["origin"]),
                'workflowInErrorSta1te': str(x["workflowInErrorState"]),
                'workflowInRetry': str(x["workflowInRetry"]),
                'lastMessageUnread': str(x["lastMessageUnread"]),
                'ShippingEstimatedDate': str(x["ShippingEstimatedDate"]),
                'ShippingEstimatedDateMax': str(x["ShippingEstimatedDateMax"]),
                'ShippingEstimatedDateMin': str(x["ShippingEstimatedDateMin"]),
                'orderIsComplete': str(x["orderIsComplete"]),
                'listId': str(x["listId"]),
                'listType': str(x["listType"]),
                'authorizedDate': str(x["authorizedDate"]),
                'callCenterOperatorName': str(x["callCenterOperatorName"]),
                'totalItems': str(x["totalItems"]),
                'currencyCode': str(x["currencyCode"])}, index=[0])
            print("Registro: "+str(init.reg))
            init.df = init.df.append(df1)
    else:
        init.salir = 1



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
        if init.salir == 1:
            break
        init.registro += 1
        get_order_list(init.registro,init.hora_0)
        get_order_list(init.registro,init.hora_1)
        get_order_list(init.registro,init.hora_2)
        get_order_list(init.registro,init.hora_3)
        get_order_list(init.registro,init.hora_4)
        get_order_list(init.registro,init.hora_5)
        get_order_list(init.registro,init.hora_6)
        get_order_list(init.registro,init.hora_7)
        get_order_list(init.registro,init.hora_8)
        get_order_list(init.registro,init.hora_9)
        get_order_list(init.registro,init.hora_10)
        get_order_list(init.registro,init.hora_11)
        get_order_list(init.registro,init.hora_13)
        get_order_list(init.registro,init.hora_14)
        get_order_list(init.registro,init.hora_15)
        get_order_list(init.registro,init.hora_16)
        get_order_list(init.registro,init.hora_17)
        get_order_list(init.registro,init.hora_18)
        get_order_list(init.registro,init.hora_19)
        get_order_list(init.registro,init.hora_20)
        get_order_list(init.registro,init.hora_21)
        get_order_list(init.registro,init.hora_22)
    
    
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
        "name": "workflowInErrorSta1te",
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
        "mode": "NULLABLE"
      }
    
    project_id = '999847639598'
    dataset_id = 'staging_zone'
    table_id = 'shopstar_vtex_list_order'
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
    job_config.schema = format_schema(table_schema)
    job = client.load_table_from_json(json_object, tableT, job_config = job_config_temp)
    print(job.result())
    delete_duplicate()

run()


