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
    hora_0 = {"f_creationDate":"creationDate:["+str(today)+"T01:00:00.000Z TO "+str(today)+"T01:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_1 = {"f_creationDate":"creationDate:["+str(today)+"T02:00:00.000Z TO "+str(today)+"T02:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_2 = {"f_creationDate":"creationDate:["+str(today)+"T03:00:00.000Z TO "+str(today)+"T03:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_3 = {"f_creationDate":"creationDate:["+str(today)+"T04:00:00.000Z TO "+str(today)+"T04:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_4 = {"f_creationDate":"creationDate:["+str(today)+"T05:00:00.000Z TO "+str(today)+"T05:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_5 = {"f_creationDate":"creationDate:["+str(today)+"T06:00:00.000Z TO "+str(today)+"T06:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_6 = {"f_creationDate":"creationDate:["+str(today)+"T07:00:00.000Z TO "+str(today)+"T07:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_7 = {"f_creationDate":"creationDate:["+str(today)+"T08:00:00.000Z TO "+str(today)+"T08:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_8 = {"f_creationDate":"creationDate:["+str(today)+"T09:00:00.000Z TO "+str(today)+"T19:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_9 = {"f_creationDate":"creationDate:["+str(today)+"T10:00:00.000Z TO "+str(today)+"T10:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_10 = {"f_creationDate":"creationDate:["+str(today)+"T11:00:00.000Z TO "+str(today)+"T11:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_11 = {"f_creationDate":"creationDate:["+str(today)+"T12:00:00.000Z TO "+str(today)+"T12:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_12 = {"f_creationDate":"creationDate:["+str(today)+"T13:00:00.000Z TO "+str(today)+"T13:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_13 = {"f_creationDate":"creationDate:["+str(today)+"T14:00:00.000Z TO "+str(today)+"T14:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_14 = {"f_creationDate":"creationDate:["+str(today)+"T15:00:00.000Z TO "+str(today)+"T15:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_15 = {"f_creationDate":"creationDate:["+str(today)+"T16:00:00.000Z TO "+str(today)+"T16:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_16 = {"f_creationDate":"creationDate:["+str(today)+"T17:00:00.000Z TO "+str(today)+"T17:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_17 = {"f_creationDate":"creationDate:["+str(today)+"T18:00:00.000Z TO "+str(today)+"T18:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_18 = {"f_creationDate":"creationDate:["+str(today)+"T19:00:00.000Z TO "+str(today)+"T19:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_19 = {"f_creationDate":"creationDate:["+str(today)+"T20:00:00.000Z TO "+str(today)+"T20:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_20 = {"f_creationDate":"creationDate:["+str(today)+"T21:00:00.000Z TO "+str(today)+"T21:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_21 = {"f_creationDate":"creationDate:["+str(today)+"T22:00:00.000Z TO "+str(today)+"T22:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_22 = {"f_creationDate":"creationDate:["+str(today)+"T23:00:00.000Z TO "+str(today)+"T23:59:59.999Z]","f_hasInputInvoice":"false"}
    hora_23 = {"f_creationDate":"creationDate:["+str(yesterday)+"T23:59:59.000Z TO "+str(today)+"02:59:59.999Z]","f_hasInputInvoice":"false"}


def get_order_list(page,hora):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders?page="+str(page)+""
        response = requests.request("GET", url, headers=init.headers, params=hora)
        FJTemp = json.loads(response.text)
        FJson = FJTemp["list"]
        for x in FJson:
            init.reg +=1
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
            print("Registro: "+str(init.reg))
            init.df = init.df.append(df1)
    except:
        print("No Data")



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
    try:
        df = init.df
        
        if df.empty:
            print('DataFrame is empty!')
        else:
            df.reset_index(drop=True, inplace=True)
            json_data = df.to_json(orient = 'records')
            json_object = json.loads(json_data)
            
            project_id = '999847639598'
            dataset_id = 'staging_zone'
            table_id = 'shopstar_vtex_list_order'
            table_temp = 'order_write'
            
            client  = bigquery.Client(project = project_id)
            
            tableO = dataset.table(table_id)
            dataset  = client.dataset(dataset_id)
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job = client.load_table_from_json(json_object, tableO, job_config = job_config)
            print(job.result())
            
            tableT = dataset.table(table_temp)
            dataset  = client.dataset(dataset_id)
            job_config_temp = bigquery.LoadJobConfig()
            job_config_temp.write_disposition = "WRITE_TRUNCATE"
            job_config_temp.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job = client.load_table_from_json(json_object, tableT, job_config = job_config_temp)
            print(job.result())
            delete_duplicate()
    except:
        print("Error.")
        logging.exception("message")

def get_params():
    print("Cargando consulta")
    for x in range(30):
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
        get_order_list(init.registro,init.hora_23)
    run()
get_params()