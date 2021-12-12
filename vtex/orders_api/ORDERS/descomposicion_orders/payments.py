import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join

class init:
  productList = []
  df = pd.DataFrame()
  id = None
  paymentSystem = None
  paymentSystemName = None
  value = None
  installments = None
  referenceValue = None
  cardHolder = None
  cardNumber = None
  firstDigits = None
  lastDigits = None
  cvv2 = None
  expireMonth = None
  expireYear = None
  url = None
  giftCardId = None
  giftCardName = None
  giftCardCaption = None
  redemptionCode = None
  group = None
  tid = None
  dueDate = None
  giftCardProvider = None
  giftCardAsDiscount = None
  koinUrl = None
  accountId = None
  parentAccountId = None
  bankIssuedInvoiceIdentificationNumber = None
  bankIssuedInvoiceIdentificationNumberFormatted = None
  bankIssuedInvoiceBarCodeNumber = None
  bankIssuedInvoiceBarCodeType = None
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
def get_order(id,reg):
    try:
        reg +=1
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        paymentData = Fjson["paymentData"]
        transactions = paymentData["transactions"]
        for x in transactions:
            payments = x["payments"]
        for pay in payments:
            init.id = pay["id"]
            init.paymentSystem = pay["paymentSystem"]
            init.paymentSystemName = pay["paymentSystemName"]
            init.value = pay["value"]
            init.installments = pay["installments"]
            init.referenceValue = pay["referenceValue"]
            init.cardHolder = pay["cardHolder"]
            init.cardNumber = pay["cardNumber"]
            init.firstDigits = pay["firstDigits"]
            init.lastDigits = pay["lastDigits"]
            init.cvv2 = pay["cvv2"]
            init.expireMonth = pay["expireMonth"]
            init.expireYear = pay["expireYear"]
            init.url = pay["url"]
            init.giftCardId = pay["giftCardId"]
            init.giftCardName = pay["giftCardName"]
            init.giftCardCaption = pay["giftCardCaption"]
            init.redemptionCode = pay["redemptionCode"]
            init.group = pay["group"]
            init.tid = pay["tid"]
            init.dueDate = pay["dueDate"]
            init.giftCardProvider = pay["giftCardProvider"]
            init.giftCardAsDiscount = pay["giftCardAsDiscount"]
            init.koinUrl = pay["koinUrl"]
            init.accountId = pay["accountId"]
            init.parentAccountId = pay["parentAccountId"]
            init.bankIssuedInvoiceIdentificationNumber = pay["bankIssuedInvoiceIdentificationNumber"]
            init.bankIssuedInvoiceIdentificationNumberFormatted = pay["bankIssuedInvoiceIdentificationNumberFormatted"]
            init.bankIssuedInvoiceBarCodeNumber = pay["bankIssuedInvoiceBarCodeNumber"]
            init.bankIssuedInvoiceBarCodeType = pay["bankIssuedInvoiceBarCodeType"]
            print("Registro: "+str(reg))
        df1 = pd.DataFrame({
            'orderId': str(id),
            'id_payments': str(init.id),
            'paymentSystem': str(init.paymentSystem),
            'paymentSystemName': str(init.paymentSystemName),
            'value': str(init.value),
            'installments': str(init.installments),
            'referenceValue': str(init.referenceValue),
            'cardHolder': str(init.cardHolder),
            'cardNumber': str(init.cardNumber),
            'firstDigits': str(init.firstDigits),
            'lastDigits': str(init.lastDigits),
            'cvv2': str(init.cvv2),
            'expireMonth': str(init.expireMonth),
            'expireYear': str(init.expireYear),
            'url': str(init.url),
            'giftCardId': str(init.giftCardId),
            'giftCardName': str(init.giftCardName),
            'giftCardCaption': str(init.giftCardCaption),
            'redemptionCode': str(init.redemptionCode),
            'group': str(init.group),
            'tid': str(init.tid),
            'dueDate': str(init.dueDate),
            'giftCardProvider': str(init.giftCardProvider),
            'giftCardAsDiscount': str(init.giftCardAsDiscount),
            'koinUrl': str(init.koinUrl),
            'accountId': str(init.accountId),
            'parentAccountId': str(init.parentAccountId),
            'bankIssuedInvoiceIdentificationNumber': str(init.bankIssuedInvoiceIdentificationNumber),
            'bankIssuedInvoiceIdentificationNumberFormatted': str(init.bankIssuedInvoiceIdentificationNumberFormatted),
            'bankIssuedInvoiceBarCodeNumber': str(init.bankIssuedInvoiceBarCodeNumber),
            'bankIssuedInvoiceBarCodeType': str(init.bankIssuedInvoiceBarCodeType)}, index=[0])
        init.df = init.df.append(df1)
    except:
        print("Vacio")
        print("Registro: "+str(reg))
        
        
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 1
    for row in rows:
        get_order(row.orderId,registro)
        registro += 1
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_order_payments` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_order_payments`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")


def run():
    get_params()
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_order_payments'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()
    
run()
