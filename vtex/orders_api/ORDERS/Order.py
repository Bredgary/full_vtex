#!/usr/bin/python
# -*- coding: latin-1 -*-
import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone

class init:
    productList = []
    df = pd.DataFrame()
    emailTracked : None
    approvedBy : None
    cancelledBy : None
    cancelReason : None
    orderId : None
    sequence : None
    marketplaceOrderId : None
    marketplaceServicesEndpoint : None
    sellerOrderId : None
    origin : None
    affiliateId : None
    salesChannel : None
    merchantName : None
    status : None
    statusDescription : None
    value : None
    creationDate : None
    lastChange : None
    orderGroup : None
    giftRegistryData : None
    marketingData : None
    callCenterOperatorData : None
    followUpEmail : None
    lastMessage : None
    hostname : None
    invoiceData : None
    openTextField : None
    roundingError : None
    orderFormId : None
    commercialConditionData : None
    isCompleted : None
    customData : None
    allowCancellation : None
    allowEdition : None
    isCheckedIn : None
    authorizedDate : None
    invoicedDate : None
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def get_order(id,reg):
    #try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        for x in Fjson:
	        df1 = pd.DataFrame({
	            'emailTracked': x["emailTracked"],
	            'approvedBy': x["approvedBy"],
	            'cancelledBy': x["cancelledBy"],
	            'cancelReason': x["cancelReason"],
	            'orderId': x["orderId"],
	            'sequence': x["sequence"],
	            'marketplaceOrderId': x["marketplaceOrderId"],
	            'marketplaceServicesEndpoint': x["marketplaceServicesEndpoint"],
	            'sellerOrderId': x["sellerOrderId"],
	            'origin': x["origin"],
	            'affiliateId': x["affiliateId"],
	            'salesChannel': x["salesChannel"],
	            'merchantName': x["merchantName"],
	            'status': x["status"],
	            'statusDescription': x["statusDescription"],
	            'value': x["value"],
	            'creationDate': x["creationDate"],
	            'lastChange': x["lastChange"],
	            'orderGroup': x["orderGroup"],
	            'giftRegistryData': x["giftRegistryData"],
	            'marketingData': x["marketingData"],
	            'callCenterOperatorData': x["callCenterOperatorData"],
	            'followUpEmail': x["followUpEmail"],
	            'lastMessage': x["lastMessage"],
	            'hostname': x["hostname"],
	            'invoiceData': x["invoiceData"],
	            'openTextField': x["openTextField"],
	            'roundingError': x["roundingError"],
	            'orderFormId': x["orderFormId"],
	            'commercialConditionData': x["commercialConditionData"],
	            'isCompleted': x["isCompleted"],
	            'customData': x["customData"],
	            'allowCancellation': x["allowCancellation"],
	            'allowEdition': x["allowEdition"],
	            'isCheckedIn': x["isCheckedIn"],
	            'authorizedDate': x["authorizedDate"],
	            'invoicedDate': x["invoicedDate"]}, index=[0])
	        init.df = init.df.append(df1)
	        print("Registro: "+str(reg))
    #except:
    #    print("Vacio")

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT orderId FROM `shopstar-datalake.landing_zone.order_write`')
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
		QUERY = (
			'CREATE OR REPLACE TABLE `shopstar-datalake.landing_zone.shopstar_vtex_order` AS SELECT DISTINCT * FROM `shopstar-datalake.landing_zone.shopstar_vtex_order`')
		query_job = client.query(QUERY)
		rows = query_job.result()
		print(rows)
	except:
		print("Consulta SQL no ejecutada")

def run():
	try:
	    get_params()
	    df = init.df
	    df.reset_index(drop=True, inplace=True)
	    json_data = df.to_json(orient = 'records')
	    json_object = json.loads(json_data)
	    print(df)
	    
	
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
	except:
		print("Error al ingesta")
    
#run()
get_params()