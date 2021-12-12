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
    changesAttachment = None
    openTextField = None
    roundingError = None
    orderFormId = None
    commercialConditionData = None
    isCompleted = None
    customData = None
    allowCancellation = None
    allowEdition = None
    isCheckedIn = None
    marketplace = None
    authorizedDate = None
    invoicedDate = None
    cancelReason = None
    subscriptionData = None
    taxData = None
    checkedInPickupPointId = None
    cancellationData = None
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    

        
def get_order(id,reg):
    try:
        reg +=1
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        #try:
        init.orderId = Fjson["orderId"]
        init.sequence = Fjson["sequence"]
        init.marketplaceOrderId = Fjson["marketplaceOrderId"]
        init.marketplaceServicesEndpoint = Fjson["marketplaceServicesEndpoint"]
        init.sellerOrderId = Fjson["sellerOrderId"]
        init.origin = Fjson["origin"]
        init.affiliateId = Fjson["affiliateId"]
        init.salesChannel = Fjson["salesChannel"]
        init.merchantName = Fjson["merchantName"]
        init.status = Fjson["status"]
        init.statusDescription = Fjson["statusDescription"]
        init.value = Fjson["value"]
        init.creationDate = Fjson["creationDate"]
        init.lastChange = Fjson["lastChange"]
        init.orderGroup = Fjson["orderGroup"]
        init.giftRegistryData = Fjson["giftRegistryData"]
        init.marketingData = Fjson["marketingData"]
        init.callCenterOperatorData = Fjson["callCenterOperatorData"]
        init.followUpEmail = Fjson["followUpEmail"]
        init.lastMessage = Fjson["lastMessage"]
        init.hostname = Fjson["hostname"]
        init.changesAttachment = Fjson["changesAttachment"]
        init.openTextField = Fjson["openTextField"]
        init.roundingError = Fjson["roundingError"]
        init.orderFormId = Fjson["orderFormId"]
        init.commercialConditionData = Fjson["commercialConditionData"]
        init.isCompleted = Fjson["isCompleted"]
        init.customData = Fjson["customData"]
        init.allowCancellation = Fjson["allowCancellation"]
        init.allowEdition = Fjson["allowEdition"]
        init.isCheckedIn = Fjson["isCheckedIn"]
        init.marketplace = Fjson["marketplace"]
        init.authorizedDate = Fjson["authorizedDate"]
        init.invoicedDate = Fjson["invoicedDate"]
        init.cancelReason = str(Fjson["cancelReason"])
        init.taxData = Fjson["taxData"]
        init.checkedInPickupPointId = Fjson["checkedInPickupPointId"]
        init.cancellationData = Fjson["cancellationData"]
        
        
        df1 = pd.DataFrame({
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
            'lastMessage': init.lastMessage,
            'hostname': init.hostname,
            'changesAttachment': init.changesAttachment,
            'openTextField': init.openTextField,
            'roundingError': init.roundingError,
            'orderFormId': init.orderFormId,
            'commercialConditionData': init.commercialConditionData,
            'isCompleted': init.isCompleted,
            'customData': init.customData,
            'allowCancellation': init.allowCancellation,
            'allowEdition': init.allowEdition,
            'isCheckedIn': init.isCheckedIn,
            'marketplace': init.marketplace,
            'authorizedDate': init.authorizedDate,
            'invoicedDate': init.invoicedDate,
            'cancelReason': init.cancelReason,
            'taxData': init.taxData,
            'checkedInPickupPointId': init.checkedInPickupPointId,
            'cancellationData': init.cancellationData,
            'followUpEmail': init.followUpEmail}, index=[0])
        init.df = init.df.append(df1)
        print("Registro: "+str(reg))
    except:
        print("vacio")
        
        
def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        get_order(row.orderId,registro)
        registro += 1
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_order_ft_order` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_order_ft_order`')
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
    table_id = 'shopstar_order_ft_order'
    
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
    
run()