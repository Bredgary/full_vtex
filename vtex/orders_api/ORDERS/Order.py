#!/usr/bin/python
# -*- coding: latin-1 -*-
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
    raw_input = ""
    emailTracked = None
    approvedBy = None
    cancelledBy = None
    cancelReason = None
    orderId = None
    sequence = None
    marketplaceOrderId = None
    marketplaceServicesEndpoint = None
    sellerOrderId = None
    origin = None
    affiliateId = None
    salesChannel = None
    merchantName = None
    status = None
    statusDescription = None
    value = None
    creationDate = None
    lastChange = None
    orderGroup = None
    giftRegistryData = None
    marketingData = None
    callCenterOperatorData = None
    followUpEmail = None
    lastMessage = None
    hostname = None
    invoiceData = None
    openTextField = None
    roundingError = None
    orderFormId = None
    commercialConditionData = None
    isCompleted = None
    customData = None
    allowCancellation = None
    allowEdition = None
    isCheckedIn = None
    authorizedDate = None
    invoicedDate = None
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def dicMemberCheck(key, dicObj):
    if key in dicObj:
        return True
    else:
        return False
        
def get_order(id,reg):
    #try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        if "emailTracked" in Fjson:
        	init.emailTracked = Fjson["emailTracked"]
        if "approvedBy" in Fjson:
        	init.approvedBy = Fjson["approvedBy"]
        if "cancelledBy" in Fjson:
        	init.cancelledBy = Fjson["cancelledBy"]
        if "cancelReason" in Fjson:
        	init.cancelReason = Fjson["cancelReason"]
        if "orderId" in Fjson:
        	init.orderId = Fjson["orderId"]
        if "sequence" in Fjson:
        	init.sequence = Fjson["sequence"]
        if "marketplaceOrderId" in Fjson:
        	init.marketplaceOrderId = Fjson["marketplaceOrderId"]
        if "marketplaceServicesEndpoint" in Fjson:
        	init.marketplaceServicesEndpoint = Fjson["marketplaceServicesEndpoint"]
        if "sellerOrderId" in Fjson:
        	init.sellerOrderId = Fjson["sellerOrderId"]
        if "origin" in Fjson:
        	init.origin = Fjson["origin"]
        if "affiliateId" in Fjson:
        	init.affiliateId = Fjson["affiliateId"]
        if "salesChannel" in Fjson:
        	init.salesChannel = Fjson["salesChannel"]
        if "merchantName" in Fjson:
        	init.merchantName = Fjson["merchantName"]
        if "status" in Fjson:
        	init.status = Fjson["status"]
        if "statusDescription" in Fjson:
        	init.statusDescription = Fjson["statusDescription"]
        if "value" in Fjson:
        	init.value = Fjson["value"]
        if "creationDate" in Fjson:
        	init.creationDate = Fjson["creationDate"]
        if "lastChange" in Fjson:
        	init.lastChange = Fjson["lastChange"]
        if "orderGroup" in Fjson:
        	init.orderGroup = Fjson["orderGroup"]
        if "giftRegistryData" in Fjson:
        	init.giftRegistryData = Fjson["giftRegistryData"]
        if "marketingData" in Fjson:
        	init.marketingData = Fjson["marketingData"]
        if "callCenterOperatorData" in Fjson:
        	init.callCenterOperatorData = Fjson["callCenterOperatorData"]
        if "followUpEmail" in Fjson:
        	init.followUpEmail = Fjson["followUpEmail"]
        if "lastMessage" in Fjson:
        	init.lastMessage = Fjson["lastMessage"]
        if "hostname" in Fjson:
        	init.hostname = Fjson["hostname"]
        if "invoiceData" in Fjson:
        	init.invoiceData = Fjson["invoiceData"]
        if "openTextField" in Fjson:
        	init.openTextField = Fjson["openTextField"]
        if "roundingError" in Fjson:
        	init.roundingError = Fjson["roundingError"]
        if "orderFormId" in Fjson:
        	init.orderFormId = Fjson["orderFormId"]
        if "commercialConditionData" in Fjson:
        	init.commercialConditionData = Fjson["commercialConditionData"]
        if "isCompleted" in Fjson:
        	init.isCompleted = Fjson["isCompleted"]
        if "customData" in Fjson:
        	init.customData = Fjson["customData"]
        if "allowCancellation" in Fjson:
        	init.allowCancellation = Fjson["allowCancellation"]
        if "allowEdition" in Fjson:
        	init.allowEdition = Fjson["allowEdition"]
        if "isCheckedIn" in Fjson:
        	init.isCheckedIn = Fjson["isCheckedIn"]
        if "authorizedDate" in Fjson:
        	init.authorizedDate = Fjson["authorizedDate"]
        if "invoicedDate" in Fjson:
        	init.invoicedDate = Fjson["invoicedDate"]

        df1 = pd.DataFrame({
            'emailTracked': init.emailTracked,
            'approvedBy': init.approvedBy,
            'cancelledBy': init.cancelledBy,
            'cancelReason': init.cancelReason,
            'orderId': init.orderId,
            'sequence': init.sequence,
            'marketplaceOrderId': init.marketplaceOrderId,
            'marketplaceServicesEndpoint': init.marketplaceServicesEndpoint,
            'sellerOrderId': init.sellerOrderId,
            'origin': init.origin,
            'affiliateId': init.affiliateId,
            'salesChannel': init.salesChannel,
            'merchantName': init.merchantName,
            'status': init.status,
            'statusDescription': init.statusDescription,
            'value': init.value,
            'creationDate': init.creationDate,
            'lastChange': init.lastChange,
            'orderGroup': init.orderGroup,
            'giftRegistryData': init.giftRegistryData,
            'marketingData': init.marketingData,
            'callCenterOperatorData': init.callCenterOperatorData,
            'followUpEmail': init.followUpEmail,
            'lastMessage': init.lastMessage,
            'hostname': init.hostname,
            'invoiceData': init.invoiceData,
            'openTextField': init.openTextField,
            'roundingError': init.roundingError,
            'orderFormId': init.orderFormId,
            'commercialConditionData': init.commercialConditionData,
            'isCompleted': init.isCompleted,
            'customData': init.customData,
            'allowCancellation': init.allowCancellation,
            'allowEdition': init.allowEdition,
            'isCheckedIn': init.isCheckedIn,
            'authorizedDate': init.authorizedDate,
            'invoicedDate': init.invoicedDate}, index=[0])
        init.df = init.df.append(df1)
        print("Registro: "+str(reg))
        
	        
    #except:
    #    print("Vacio")

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT orderId FROM `shopstar-datalake.landing_zone.shopstar_vtex_order_test`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 1
    for row in rows:
        get_order(row.orderId,registro)
        registro += 1
        break

def delete_duplicate():
	try:
		print("Eliminando duplicados")
		client = bigquery.Client()
		QUERY = (
			'CREATE OR REPLACE TABLE `shopstar-datalake.landing_zone.shopstar_vtex_order` AS SELECT DISTINCT * FROM `shopstar-datalake.landing_zone.shopstar_vtex_order`')
		query_job = client.query(QUERY)
		rows = query_job.result()
		print(rows)
	except:
		print("Consulta SQL no ejecutada")

def write(obj):
        solditems = requests.get('https://github.com/Bredgary/full_vtex/tree/main/ui/order.json')
        data = obj
        with open('data.json', 'w') as f:
            json.dump(data, f)

def run():
    #try:
    get_params()
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    write(json_object)
    
    
    '''
    project_id = '999847639598'
    dataset_id = 'landing_zone'
    table_id = 'shopstar_vtex_order'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()
    '''
    #except:
    #     print("Error")
    
run()