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
from datetime import date, timedelta

class init:
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    before_yesterday = today - datetime.timedelta(days=2)
    ordenes = {}
    df = pd.DataFrame()
    registro = 0
    reg = 0 
    headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def get_order_list(page):
    #try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders?page="+str(page)+""
        querystring = {"f_creationDate":"creationDate:[2019-01-01T02:00:00.000Z TO 2022-01-18T01:59:59.999Z]","f_hasInputInvoice":"true"}
        response = requests.request("GET", url, headers=init.headers, params=querystring)
        FJTemp = json.loads(response.text)
        FJson = FJTemp["list"]
        for x in FJson:
            init.reg +=1
            orderId = x["orderId"]
            creationDate = x["creationDate"]
            clientName = x["clientName"]
            totalValue = x["totalValue"]
            paymentNames = x["paymentNames"]
            status = x["status"]
            statusDescription = x["statusDescription"]
            marketPlaceOrderId = x["marketPlaceOrderId"]
            sequence = x["sequence"]
            salesChannel = x["salesChannel"]
            affiliateId = x["affiliateId"]
            origin = x["origin"]
            workflowInErrorState = x["workflowInErrorState"]
            workflowInRetry = x["workflowInRetry"]
            lastMessageUnread = x["lastMessageUnread"]
            ShippingEstimatedDate = x["ShippingEstimatedDate"]
            ShippingEstimatedDateMax = x["ShippingEstimatedDateMax"]
            ShippingEstimatedDateMin = x["ShippingEstimatedDateMin"]
            orderIsComplete = x["orderIsComplete"]
            listId = x["listId"]
            listType = x["listType"]
            authorizedDate = x["authorizedDate"]
            callCenterOperatorName = x["callCenterOperatorName"]
            totalItems = x["totalItems"]
            currencyCode = x["currencyCode"]
            df1 = pd.DataFrame({
                'orderId': orderId,
                'creationDate': creationDate,
                'clientName': clientName,
                'totalValue': totalValue,
                'paymentNames': paymentNames,
                'status': status,
                'statusDescription': statusDescription,
                'marketPlaceOrderId': marketPlaceOrderId,
                'sequence': sequence,
                'salesChannel': salesChannel,
                'affiliateId': affiliateId,
                'origin': origin,
                'workflowInErrorState': workflowInErrorState,
                'workflowInRetry': workflowInRetry,
                'lastMessageUnread': lastMessageUnread,
                'ShippingEstimatedDate': ShippingEstimatedDate,
                'ShippingEstimatedDateMax': ShippingEstimatedDateMax,
                'ShippingEstimatedDateMin': ShippingEstimatedDateMin,
                'orderIsComplete': orderIsComplete,
                'listId': listId,
                'listType': listType,
                'authorizedDate': authorizedDate,
                'callCenterOperatorName': callCenterOperatorName,
                'totalItems': totalItems,
                'currencyCode': currencyCode}, index=[0])
            print("Registro: "+str(init.reg))
            init.df = init.df.append(df1)
    #except:
    #    print("No Data")



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
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    table_schema =[{
        "name": "currencyCode",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "totalItems",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "workflowInErrorSta1te",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "callCenterOperatorName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "marketPlaceOrderId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "listType",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "authorizedDate",
        "type": "DATE",
        "mode": "NULLABLE"
    },{
        "name": "orderIsComplete",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "listId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "ShippingEstimatedDateMin",
        "type": "DATE",
        "mode": "NULLABLE"
    },{
        "name": "ShippingEstimatedDate",
        "type": "DATE",
        "mode": "NULLABLE"
    },{
        "name": "origin",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "affiliateId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "sequence",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "paymentNames",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "salesChannel",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "status",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "workflowInRetry",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "clientName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "ShippingEstimatedDateMax",
        "type": "DATE",
        "mode": "NULLABLE"
    },{
        "name": "creationDate",
        "type": "DATE",
        "mode": "NULLABLE"
    },{
        "name": "lastMessageUnread",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "totalValue",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "statusDescription",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "orderId",
        "type": "STRING",
        "mode": "NULLABLE"
    }]

    
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
    job_config_temp.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = format_schema(table_schema)
    job = client.load_table_from_json(json_object, tableT, job_config = job_config_temp)
    print(job.result())
    delete_duplicate()

def get_params():
    for x in range(1):
        init.registro += 1
        get_order_list(init.registro)


get_params()