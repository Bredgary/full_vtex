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
        
        if Fjson["changesAttachment"] is not None:
            changesAttachment = Fjson["changesAttachment"]
            changesAttachment_id = changesAttachment["id"]
        else:
            changesAttachment_id = ""
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
        invoicedDate = Fjson["invoicedDate"]
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
        
        for x in sellers:
            seller_id = x["id"]
            seller_name = x["name"]
            seller_logo = x["logo"]
            seller_fulfillmentEndpoint = x["fulfillmentEndpoint"]
    
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
            RequestedByPaymentNotification = None
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
        
        if df.empty:
            print('DataFrame is empty!')
        else:
            client  = bigquery.Client(project = project_id)
            dataset  = client.dataset(dataset_id)
            table = dataset.table(table_id)
            job_config = bigquery.LoadJobConfig()
            job_config.schema = format_schema(table_schema)
            #job_config.write_disposition = "WRITE_TRUNCATE"
            #job_config.autodetect = True
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
    run()

get_params()
