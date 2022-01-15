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
    
    df = pd.DataFrame()
    registro = 0
    reg = 0
    headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}



def get_order_list(page,hora):
    #try:
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
            if init.reg == 2:
                run()
            if init.reg == 300:
                run()
            if init.reg == 400:
                run()
            if init.reg == 500:
                run()
            if init.reg == 600:
                run()
            if init.reg == 700:
                run()
            if init.reg == 800:
                run()
            if init.reg == 900:
                run()
            if init.reg == 1000:
                run()
            if init.reg == 1100:
                run()
            if init.reg == 1200:
                run()
            if init.reg == 1300:
                run()
            if init.reg == 1400:
                run()
            if init.reg == 1500:
                run()
            if init.reg == 10000:
                run()
            if init.reg == 15000:
                run()
            if init.reg == 20000:
                run()
            if init.reg == 25000:
                run()
            if init.reg == 30000:
                run()
            if init.reg == 35000:
                run()
            if init.reg == 40000:
                run()
            if init.reg == 45000:
                run()
            if init.reg == 50000:
                run()
            if init.reg == 55000:
                run()
            if init.reg == 60000:
                run()
            if init.reg == 65000:
                run()
            if init.reg == 70000:
                run()
            if init.reg == 75000:
                run()
            if init.reg == 80000:
                run()
            if init.reg == 85000:
                run()
            if init.reg == 90000:
                run()
    #except:
     #   print("Vacio")



def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_list_order` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

def get_params(params):
    for x in range(30):
        init.registro += 1
        get_order_list(init.registro,params)
    

def run():
    
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_vtex_list_order'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def time():
    start_date = date(2019, 1, 1)
    end_date = date(2022, 1, 15)
    for single_date in daterange(start_date, end_date):
        today = single_date.strftime("%Y-%m-%d")
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
        init.registro = 0
        get_params(hora_0)
        init.registro = 0
        get_params(hora_1)
        init.registro = 0
        get_params(hora_2)
        init.registro = 0
        get_params(hora_3)
        init.registro = 0
        get_params(hora_4)
        init.registro = 0
        get_params(hora_5)
        init.registro = 0
        get_params(hora_6)
        init.registro = 0
        get_params(hora_7)
        init.registro = 0
        get_params(hora_8)
        init.registro = 0
        get_params(hora_9)
        init.registro = 0
        get_params(hora_10)
        init.registro = 0
        get_params(hora_11)
        init.registro = 0
        get_params(hora_12)
        init.registro = 0
        get_params(hora_13)
        init.registro = 0
        get_params(hora_14)
        init.registro = 0
        get_params(hora_15)
        init.registro = 0
        get_params(hora_16)
        init.registro = 0
        get_params(hora_17)
        init.registro = 0
        get_params(hora_18)
        init.registro = 0
        get_params(hora_19)
        init.registro = 0
        get_params(hora_20)
        init.registro = 0
        get_params(hora_21)
        init.registro = 0
        get_params(hora_22)
        init.registro = 0
    run()
    
time()





