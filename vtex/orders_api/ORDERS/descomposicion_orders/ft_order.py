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
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def format_schema(schema):
  formatted_schema = []
  for row in schema:
    formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def decrypt_email(email):
  try:
    url = "https://conversationtracker.vtex.com.br/api/pvt/emailMapping?an=mercury&alias="+email+""
    response = requests.request("GET", url, headers=init.headers)
    formatoJ = json.loads(response.text)
    return formatoJ["email"]
  except:
    print(str(email))

def get_order(id):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        clientProfileData = Fjson["clientProfileData"]
        client_email = clientProfileData["email"]
        orderId = Fjson["orderId"]
        client_email = clientProfileData["email"]
        userProfileId = clientProfileData["userProfileId"]
        sequence = Fjson["sequence"]
        marketplaceOrderId = Fjson["marketplaceOrderId"]
        marketplaceServicesEndpoint = Fjson["marketplaceServicesEndpoint"]
        sellerOrderId = Fjson["sellerOrderId"]
        origin = Fjson["origin"]
        affiliateId = Fjson["affiliateId"]
        checkedInPickupPointId = Fjson["checkedInPickupPointId"]
        salesChannel = Fjson["salesChannel"]
        merchantName = Fjson["merchantName"]
        status = Fjson["status"]
        statusDescription = Fjson["statusDescription"]
        value = Fjson["value"]
        creationDate = Fjson["creationDate"]
        lastChange = Fjson["lastChange"]
        orderGroup  = Fjson["orderGroup"]
        callCenterOperatorData = Fjson["callCenterOperatorData"]
        followUpEmail = Fjson["followUpEmail"]
        lastMessage = Fjson["lastMessage"]
        hostname = Fjson["hostname"]
        changesAttachment = Fjson["changesAttachment"]
        openTextField = Fjson["openTextField"]
        roundingError = Fjson["roundingError"]
        orderFormId = Fjson["orderFormId"]
        commercialConditionData = Fjson["commercialConditionData"]
        isCompleted = Fjson["isCompleted"]
        customData = Fjson["customData"]
        allowCancellation = Fjson["allowCancellation"]
        allowEdition = Fjson["allowEdition"]
        isCheckedIn = Fjson["isCheckedIn"]
        marketplace = Fjson["marketplace"]
        authorizedDate = Fjson["authorizedDate"]
        invoicedDate = Fjson["invoicedDate"]
        cancelReason = Fjson["cancelReason"]
        subscriptionData = Fjson["subscriptionData"]
        taxData = Fjson["taxData"]
        giftRegistryData = Fjson["giftRegistryData"]
        client_email = decrypt_email(str(client_email))
        
        try:
            marketplaceOrderId = Fjson["marketplaceOrderId"]
            baseURL = marketplace["baseURL"]
            isCertified = marketplace["isCertified"]
            name = marketplace["name"]
        except:
            baseURL = ''
            isCertified = ''
            name = ''
        try:
            sellers_ = Fjson["sellers"]
            sellers = sellers_[0]
            seller_id = sellers["id"]
            seller_name = sellers["name"]
            seller_logo = sellers["logo"]
        except:
            seller_id = ''
            seller_name = ''
            seller_logo = ''
        try:
            cancellationData = Fjson["cancellationData"]
            CancellationDate = cancellationData["CancellationDate"]
        except:
            cache=2
        try:
            RequestedByUser = cancellationData["RequestedByUser"]
            RequestedBySystem = cancellationData["RequestedBySystem"]
            RequestedBySellerNotification = cancellationData["RequestedBySellerNotification"]
            RequestedByPaymentNotification = cancellationData["RequestedByPaymentNotification"]
            Reason = cancellationData["Reason"]
        except:
            RequestedByUser = ""
            RequestedBySystem = False
            RequestedBySellerNotification = False
            RequestedByPaymentNotification = False
            Reason = ""
            CancellationDate = ""
        
        try:
            dim_invoiceData = Fjson["invoiceData"]
            invoice_address = dim_invoiceData["address"]
            userPaymentInfo = dim_invoiceData["userPaymentInfo"]
        except:
            invoice_address = ''
            userPaymentInfo = ''
            
        df1 = pd.DataFrame({
            'orderId': id,
            'client_email': client_email,
            'userProfileId': userProfileId,
            'sequence': sequence,
            'marketplaceOrderId': marketplaceOrderId,
            'marketplaceServicesEndpoint': marketplaceServicesEndpoint,
            'sellerOrderId':sellerOrderId,
            'origin': origin,
            'affiliateId': affiliateId,
            'checkedInPickupPointId': checkedInPickupPointId,
            'salesChannel': salesChannel,
            'merchantName': merchantName,
            'status': status,
            'statusDescription': statusDescription,
            'value': value,
            'creationDate': creationDate,
            'lastChange': lastChange,
            'orderGroup': orderGroup,
            'callCenterOperatorData': callCenterOperatorData,
            'followUpEmail': followUpEmail,
            'lastMessage': lastMessage,
            'hostname': hostname,
            'changesAttachment': changesAttachment,
            'openTextField': openTextField,
            'roundingError': roundingError,
            'orderFormId': orderFormId,
            'commercialConditionData': commercialConditionData,
            'isCompleted': isCompleted,
            'customData': customData,
            'allowCancellation': allowCancellation,
            'allowEdition': allowEdition,
            'isCheckedIn': isCheckedIn,
            'marketplace': marketplace,
            'authorizedDate': authorizedDate,
            'invoicedDate': invoicedDate,
            'cancelReason': cancelReason,
            'subscriptionData': subscriptionData,
            'taxData': taxData,
            'baseURL': baseURL,
            'isCertified': isCertified,
            'name': name,
            'seller_id': seller_id,
            'seller_name': seller_name,
            'seller_logo': seller_logo,
            'CancellationDate': CancellationDate,
            'RequestedByUser': RequestedByUser,
            'RequestedBySystem': RequestedBySystem,
            'RequestedBySellerNotification': RequestedBySellerNotification,
            'RequestedByPaymentNotification': RequestedByPaymentNotification,
            'Reason': Reason,
            'giftRegistryData': giftRegistryData}, index=[0])
        init.df = init.df.append(df1)
    except:
        print("vacio")

def delete_duplicate():
  try:
    print("Eliminando duplicados")
    client = bigquery.Client()
    QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_ft_orders` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_ft_orders`')
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
  
  table_schema = [
      {
          "name": "giftRegistryData",
          "type": "STRING",
          "mode": "NULLABLE"
  },{
      "name": "RequestedByPaymentNotification",
      "type": "BOOLEAN",
      "mode": "NULLABLE"
  },{
      "name": "RequestedBySellerNotification",
      "type": "BOOLEAN",
      "mode": "NULLABLE"
  },{
      "name": "RequestedByUser",
      "type": "FLOAT",
      "mode": "NULLABLE"
  },{
      "name": "RequestedBySystem",
      "type": "BOOLEAN",
      "mode": "NULLABLE"
  },{
      "name": "CancellationDate",
      "type": "FLOAT",
      "mode": "NULLABLE"
  },{
      "name": "seller_name",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "seller_id",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "baseURL",
      "type": "FLOAT",
      "mode": "NULLABLE"
  },{
      "name": "name",
      "type": "FLOAT",
      "mode": "NULLABLE"
  },{
      "name": "changesAttachment",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "lastChange",
      "type": "DATE",
      "mode": "NULLABLE"
  },{
      "name": "isCertified",
      "type": "FLOAT",
      "mode": "NULLABLE"
  },{
      "name": "lastMessage",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "authorizedDate",
      "type": "DATE",
      "mode": "NULLABLE"
  },{
      "name": "allowEdition",
      "type": "BOOLEAN",
      "mode": "NULLABLE"
  },{
      "name": "allowCancellation",
      "type": "BOOLEAN",
      "mode": "NULLABLE"
  },{
      "name": "status",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "isCheckedIn",
      "type": "BOOLEAN",
      "mode": "NULLABLE"
  },{
      "name": "subscriptionData",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "commercialConditionData",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "isCompleted",
      "type": "BOOLEAN",
      "mode": "NULLABLE"
  },{
      "name": "roundingError",
      "type": "INTEGER",
      "mode": "NULLABLE"
  },{
      "name": "orderId",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "marketplace",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "callCenterOperatorData",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "orderGroup",
      "type": "INTEGER",
      "mode": "NULLABLE"
  },{
      "name": "creationDate",
      "type": "DATE",
      "mode": "NULLABLE"
  },{
      "name": "cancelReason",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "orderFormId",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "seller_logo",
      "type": "FLOAT",
      "mode": "NULLABLE"
  },{
      "name": "sellerOrderId",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "statusDescription",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "value",
      "type": "INTEGER",
      "mode": "NULLABLE"
  },{
      "name": "invoicedDate",
      "type": "DATE",
      "mode": "NULLABLE"
  },{
      "name": "customData",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "merchantName",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "sequence",
      "type": "INTEGER",
      "mode": "NULLABLE"
  },{
      "name": "affiliateId",
      "type": "FLOAT",
      "mode": "NULLABLE"
  },{
      "name": "followUpEmail",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "userProfileId",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "hostname",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "checkedInPickupPointId",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "origin",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "salesChannel",
      "type": "INTEGER",
      "mode": "NULLABLE"
  },{
      "name": "marketplaceServicesEndpoint",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "Reason",
      "type": "FLOAT",
      "mode": "NULLABLE"
  },{
      "name": "client_email",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "taxData",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "openTextField",
      "type": "STRING",
      "mode": "NULLABLE"
  },{
      "name": "marketplaceOrderId",
      "type": "FLOAT",
      "mode": "NULLABLE"
  }]

  
  project_id = '999847639598'
  dataset_id = 'test'
  table_id = 'shopstar_vtex_ft_orders'
  
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
      job_config.autodetect = True
      job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
      job = client.load_table_from_json(json_object, table, job_config = job_config)
      print(job.result())
      delete_duplicate()   


def get_params():
  print("Cargando consulta")
  client = bigquery.Client()
  QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_vtex_ft_orders`))')
  query_job = client.query(QUERY)  
  rows = query_job.result()
  registro = 0
  for row in rows:
    registro += 1
    get_order(row.orderId)
    print("Registro: "+str(registro))
  run()

get_params()
