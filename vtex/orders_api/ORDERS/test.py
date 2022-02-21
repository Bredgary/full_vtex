import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join
import logging

class init:
    productList = []
    df = pd.DataFrame()
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
def get_order(id,reg):
    try:
        df1 = pd.DataFrame
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        paymentData = Fjson["paymentData"]
        transactions = paymentData["transactions"]
        for x in transactions:
            payments = x["payments"]
            for pay in payments:
                id_payments = pay["id"]
                paymentSystem = pay["paymentSystem"]
                paymentSystemName = pay["paymentSystemName"]
                value = pay["value"]
                installments = pay["installments"]
                referenceValue = pay["referenceValue"]
                cardHolder = pay["cardHolder"]
                cardNumber = pay["cardNumber"]
                firstDigits = pay["firstDigits"]
                lastDigits = pay["lastDigits"]
                cvv2 = pay["cvv2"]
                expireMonth = pay["expireMonth"]
                expireYear = pay["expireYear"]
                url = pay["url"]
                giftCardId = pay["giftCardId"]
                giftCardName = pay["giftCardName"]
                giftCardCaption = pay["giftCardCaption"]
                redemptionCode = pay["redemptionCode"]
                group = pay["group"]
                tid = pay["tid"]
                dueDate = pay["dueDate"]
                giftCardProvider = pay["giftCardProvider"]
                giftCardAsDiscount = pay["giftCardAsDiscount"]
                koinUrl = pay["koinUrl"]
                accountId = pay["accountId"]
                parentAccountId = pay["parentAccountId"]
                bankIssuedInvoiceIdentificationNumber = pay["bankIssuedInvoiceIdentificationNumber"]
                bankIssuedInvoiceIdentificationNumberFormatted = pay["bankIssuedInvoiceIdentificationNumberFormatted"]
                bankIssuedInvoiceBarCodeNumber = pay["bankIssuedInvoiceBarCodeNumber"]
                bankIssuedInvoiceBarCodeType = pay["bankIssuedInvoiceBarCodeType"]
                
                df1 = pd.DataFrame({
                    'orderId': id,
                    'id_payments': id_payments,
                    'paymentSystem': paymentSystem,
                    'paymentSystemName': paymentSystemName,
                    'value': value,
                    'installments': installments,
                    'referenceValue': referenceValue,
                    'cardHolder': cardHolder,
                    'cardNumber': cardNumber,
                    'firstDigits': firstDigits,
                    'lastDigits': lastDigits,
                    'cvv2': cvv2,
                    'expireMonth': expireMonth,
                    'expireYear': expireYear,
                    'url': url,
                    'giftCardId': giftCardId,
                    'giftCardName': giftCardName,
                    'giftCardCaption': giftCardCaption,
                    'redemptionCode': redemptionCode,
                    'group': group,
                    'tid': tid,
                    'dueDate': dueDate,
                    'giftCardProvider': giftCardProvider,
                    'giftCardAsDiscount': giftCardAsDiscount,
                    'koinUrl': koinUrl,
                    'accountId': accountId,
                    'parentAccountId': parentAccountId,
                    'bankIssuedInvoiceIdentificationNumber': bankIssuedInvoiceIdentificationNumber,
                    'bankIssuedInvoiceIdentificationNumberFormatted': bankIssuedInvoiceIdentificationNumberFormatted,
                    'bankIssuedInvoiceBarCodeNumber': bankIssuedInvoiceBarCodeNumber,
                    'bankIssuedInvoiceBarCodeType': bankIssuedInvoiceBarCodeType}, index=[0])
                init.df = init.df.append(df1)
                print("Registro: "+str(reg))
        if df.empty:
            df1 = pd.DataFrame({
                'orderId': id
                }, index=[0])
            init.df = init.df.append(df1)
    except:
        df1 = pd.DataFrame({
            'orderId': id}, index=[0])
        init.df = init.df.append(df1)
        
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_order_payments` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_order_payments`')
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
        
        table_schema = [{
            "name": "bankIssuedInvoiceBarCodeNumber",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "bankIssuedInvoiceIdentificationNumberFormatted",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "bankIssuedInvoiceIdentificationNumber",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "accountId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "parentAccountId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "koinUrl",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "giftCardAsDiscount",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "dueDate",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "group",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "giftCardCaption",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "giftCardName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "expireYear",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "giftCardId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "cardHolder",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "cardNumber",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "referenceValue",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "cvv2",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "lastDigits",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "bankIssuedInvoiceBarCodeType",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "paymentSystem",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "url",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "value",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "firstDigits",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "orderId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "paymentSystemName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "redemptionCode",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "giftCardProvider",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "tid",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "id_payments",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "installments",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "expireMonth",
            "type": "STRING",
            "mode": "NULLABLE"
        }]
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_order_payments'
        
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
    QUERY = ('SELECT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_order_payments`))')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId,registro)
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
