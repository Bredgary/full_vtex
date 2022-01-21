import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join
from _queue import Empty
import datetime
import time
import logging

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
        try:
            clientProfileData = Fjson["clientProfileData"]
            client_email = clientProfileData["email"]
            client_email = decrypt_email(str(client_email))
            userProfileId = clientProfileData["userProfileId"]
        except:
            client_email = None
            userProfileId = None
        orderId = Fjson["orderId"]
        sequence = Fjson["sequence"]
        try:
            marketplaceOrderId = Fjson["marketplaceOrderId"]
        except:
            marketplaceOrderId = None
        marketplaceServicesEndpoint = Fjson["marketplaceServicesEndpoint"]
        sellerOrderId = Fjson["sellerOrderId"]
        origin = Fjson["origin"]
        try:
            affiliateId = Fjson["affiliateId"]
        except:
            affiliateId = None
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
        
        if Fjson["changesAttachment"] is not None:
            changesAttachment = Fjson["changesAttachment"]
            changesAttachment_id = changesAttachment["id"]
        else:
            changesAttachment_id = None
        openTextField = Fjson["openTextField"]
        roundingError = Fjson["roundingError"]
        orderFormId = Fjson["orderFormId"]
        commercialConditionData = Fjson["commercialConditionData"]
        isCompleted = Fjson["isCompleted"]
        customData = Fjson["customData"]
        allowCancellation = Fjson["allowCancellation"]
        allowEdition = Fjson["allowEdition"]
        isCheckedIn = Fjson["isCheckedIn"]
        authorizedDate = Fjson["authorizedDate"]
        try:
            invoicedDate = Fjson["invoicedDate"]
        except:
            invoicedDate = '1900-01-01 15:15:18.051893 UTC'
        cancelReason = Fjson["cancelReason"]
        subscriptionData = Fjson["subscriptionData"]
        taxData = Fjson["taxData"]
        giftRegistryData = Fjson["giftRegistryData"]
        
        Total = Fjson["totals"]
        
        if Total[0] is not None:
            items = Total[0]
            total_id_items = items["id"]
            total_name_items = items["name"]
            total_value_items = items["value"]
        else:
            total_id_items = None
            total_name_items = None
            total_value_items = None
            
        if Total[1] is not None:
            discounts = Total[1]
            total_id_discounts = discounts["id"]
            total_name_discounts = discounts["name"]
            total_value_discounts = discounts["value"]
        else:
            total_id_discounts = None
            total_name_discounts = None
            total_value_discounts = None
    
        if Total[2] is not None:
            shipping = Total[2]
            total_id_shipping = shipping["id"]
            total_name_shipping = shipping["name"]
            total_value_shipping = shipping["value"]
        else:
            total_id_shipping = None
            total_name_shipping = None
            total_value_shipping = None
            
        if Total[3] is not None:
            tax = Total[3]
            total_id_tax = tax["id"]
            total_name_tax = tax["name"]
            total_value_tax = tax["value"]
        else:
            total_id_tax = None
            total_name_tax = None
            total_value_tax = None
    
        if Fjson["shippingData"]:
            shippingData = Fjson["shippingData"]
            shippingData_id = shippingData["id"]
        else:
            shippingData_id = None
    
        if Fjson["marketplace"] is not None:
            marketplace = Fjson["marketplace"]
            baseURL = marketplace["baseURL"]
            isCertified = marketplace["isCertified"]
            name = marketplace["name"]
        else:
            baseURL = None
            isCertified = False
            name = None
    
        sellers = Fjson["sellers"]
        
        try:
            for x in sellers:
                seller_id = x["id"]
                seller_name = x["name"]
                seller_logo = x["logo"]
                seller_fulfillmentEndpoint = x["fulfillmentEndpoint"]
        except:
            seller_id = None
            seller_name = None
            seller_logo = None
            seller_fulfillmentEndpoint = None
    
        if Fjson["cancellationData"] is not None:
            cancellationData = Fjson["cancellationData"]
            CancellationDate = cancellationData["CancellationDate"]
        else:
            CancellationDate = '1900-01-01 15:15:18.051893 UTC'
        
        if Fjson["cancellationData"] is not None:
            cancellationData = Fjson["cancellationData"]
            RequestedByUser = cancellationData["RequestedByUser"]
            RequestedBySystem = cancellationData["RequestedBySystem"]
            RequestedBySellerNotification = cancellationData["RequestedBySellerNotification"]
            RequestedByPaymentNotification = cancellationData["RequestedByPaymentNotification"]
            Reason = cancellationData["Reason"]
        else:
            RequestedByUser = False
            RequestedBySystem = False
            RequestedBySellerNotification = False
            RequestedByPaymentNotification = False
            Reason = None
        
        if Fjson["invoiceData"] is not None:
            dim_invoiceData = Fjson["invoiceData"]
            invoice_address = dim_invoiceData["address"]
            userPaymentInfo = dim_invoiceData["userPaymentInfo"]
        else:
            invoice_address = None
            userPaymentInfo = None
            
        if Fjson["shippingData"] is not None:
            shippingData = Fjson["shippingData"]
            address = shippingData["address"]
            shipping_addressType = address["addressType"]
            shipping_receiverName = address["receiverName"]
            shipping_addressId = address["addressId"]
            shipping_postalCode = address["postalCode"]
            shipping_city = address["city"]
            shipping_state = address["state"]
            shipping_country = address["country"]
            shipping_street = address["street"]
            shipping_number = address["number"]
            shipping_neighborhood = address["neighborhood"]
            shipping_complement = address["complement"]
            shipping_reference = address["reference"]
        else:
            shipping_addressType = None
            shipping_receiverName = None
            shipping_addressId = None
            shipping_postalCode = None
            shipping_city = None
            shipping_state = None
            shipping_country = None
            shipping_street = None
            shipping_number = None
            shipping_neighborhood = None
            shipping_complement = None
            shipping_reference = None
            
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
            'changesAttachment_id': changesAttachment_id,
            'openTextField': openTextField,
            'roundingError': roundingError,
            'orderFormId': orderFormId,
            'commercialConditionData': commercialConditionData,
            'isCompleted': isCompleted,
            'customData': customData,
            'allowCancellation': allowCancellation,
            'allowEdition': allowEdition,
            'isCheckedIn': isCheckedIn,
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
            'seller_fulfillmentEndpoint': seller_fulfillmentEndpoint,
            'CancellationDate': CancellationDate,
            'RequestedByUser': RequestedByUser,
            'RequestedBySystem': RequestedBySystem,
            'RequestedBySellerNotification': RequestedBySellerNotification,
            'RequestedByPaymentNotification': RequestedByPaymentNotification,
            'Reason': Reason,
            'giftRegistryData': giftRegistryData,
            'totals_id_items': total_id_items,
            'totals_name_items': total_name_items,
            'totals_value_items': total_value_items,
            'totals_id_discounts': total_id_discounts,
            'totals_name_discounts': total_name_discounts,
            'totals_value_discounts': total_value_discounts,
            'totals_id_shipping': total_id_shipping,
            'totals_name_shipping': total_name_shipping,
            'totals_value_shipping': total_value_shipping,
            'totals_id_tax': total_id_tax,
            'totals_name_tax': total_name_tax,
            'totals_value_tax': total_value_tax,
            'invoice_address': invoice_address,
            'userPaymentInfo': userPaymentInfo,
            'shippingData_id': shippingData_id,
            'shipping_addressType': shipping_addressType,
            'shipping_receiverName': shipping_receiverName,
            'shipping_addressId': shipping_addressId,
            'shipping_postalCode': shipping_postalCode,
            'shipping_city': shipping_city,
            'shipping_state': shipping_state,
            'shipping_country': shipping_country,
            'shipping_street': shipping_street,
            'shipping_number': shipping_number,
            'shipping_neighborhood': shipping_neighborhood,
            'shipping_complement': shipping_complement,
            'shipping_reference': shipping_reference}, index=[0])
        init.df = init.df.append(df1)
    except:
        print("Vacio")

def delete_duplicate():
  try:
    print("Eliminando duplicados")
    client = bigquery.Client()
    QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_ft_orders` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_ft_orders`')
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
        
        project_id = '999847639598'
        dataset_id = 'test'
        table_id = 'shopstar_ft_orders'
        
        '''
        table_schema = [{
            "name": "shipping_neighborhood",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "shipping_state",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "shipping_city",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "shipping_postalCode",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "shipping_addressId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "shipping_addressType",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "shippingData_id",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "invoice_address",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "totals_name_tax",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "totals_id_tax",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "totals_id_shipping",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "shipping_country",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "totals_value_discounts",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "totals_name_discounts",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "totals_value_items",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "giftRegistryData",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "RequestedByPaymentNotification",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "totals_id_discounts",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "RequestedByUser",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "RequestedBySystem",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "CancellationDate",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },{
            "name": "totals_id_items",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "baseURL",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "name",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "lastChange",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },{
            "name": "isCertified",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "totals_name_items",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "lastMessage",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "authorizedDate",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },{
            "name": "allowEdition",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "totals_value_tax",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "allowCancellation",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "shipping_complement",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "shipping_street",
            "type": "STRING",
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
            "name": "shipping_receiverName",
            "type": "STRING",
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
            "name": "changesAttachment_id",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "callCenterOperatorData",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "userPaymentInfo",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "orderGroup",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "creationDate",
            "type": "TIMESTAMP",
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
            "name": "totals_value_shipping",
            "type": "INTEGER",
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
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },{
            "name": "customData",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "shipping_reference",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "merchantName",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "affiliateId",
            "type": "STRING",
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
            "name": "totals_name_shipping",
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
            "name": "RequestedBySellerNotification",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "Reason",
            "type": "STRING",
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
            "name": "shipping_number",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "openTextField",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "marketplaceOrderId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "sequence",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "orderId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "seller_id",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "seller_name",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "seller_logo",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "seller_fulfillmentEndpoint",
            "type": "STRING",
            "mode": "NULLABLE"
        }]
        '''
 
 

        
        if df.empty:
            print('DataFrame is empty!')
        else:
            client  = bigquery.Client(project = project_id)
            dataset  = client.dataset(dataset_id)
            table = dataset.table(table_id)
            job_config = bigquery.LoadJobConfig()
            #job_config.schema = format_schema(table_schema)
            job_config.write_disposition = "WRITE_TRUNCATE"
            job_config.autodetect = True
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
    QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.test.shopstar_ft_orders`))')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId)
        print("Registro: "+str(registro))
        if registro == 1:
            run()
        if registro == 50:
            run()
        if registro == 100:
            run()
        if registro == 150:
            run()
        if registro == 200:
            run()
        if registro == 200:
            run()
        if registro == 300:
            run()
        if registro == 400:
            run()
        if registro == 500:
            run()
        if registro == 600:
            run()
        if registro == 700:
            run()
        if registro == 800:
            run()
        if registro == 900:
            run()
        if registro == 1000:
            run()
        if registro == 1100:
            run()
        if registro == 1200:
            run()
        if registro == 1300:
            run()
        if registro == 1400:
            run()
        if registro == 1500:
            run()
        if registro == 2000:
            run()
        if registro == 2500:
            run()
        if registro == 3000:
            run()
        if registro == 3500:
            run()
        if registro == 4000:
            run()
        if registro == 4500:
            run()
        if registro == 5000:
            run()
        if registro == 5500:
            run()
        if registro == 6000:
            run()
        if registro == 6500:
            run()
        if registro == 7000:
            run()
        if registro == 7500:
            run()
        if registro == 8000:
            run()
        if registro == 8500:
            run()
        if registro == 9000:
            run()
        if registro == 9500:
            run()
        if registro == 10000:
            run()
        if registro == 15000:
            run()
        if registro == 20000:
            run()
        if registro == 25000:
            run()
        if registro == 30000:
            run()
        if registro == 35000:
            run()
        if registro == 40000:
            run()
        if registro == 45000:
            run()
        if registro == 50000:
            run()
        if registro == 55000:
            run()
        if registro == 60000:
            run()
        if registro == 65000:
            run()
        if registro == 70000:
            run()
        if registro == 75000:
            run()
        if registro == 80000:
            run()
        if registro == 85000:
            run()
        if registro == 90000:
            run()
        if registro == 95000:
            run()
        if registro == 100000:
            run()
        if registro == 105000:
            run()
        if registro == 110000:
            run()
        if registro == 115000:
            run()
        if registro == 120000:
            run()
        if registro == 125000:
            run()
    run()

get_params()
