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
    
    '''
    Dimensiones TOTAL
    '''
    
    total_id_items = None
    total_name_items = None
    total_value_items = None
    total_id_discounts = None
    total_name_discounts = None
    total_value_discounts = None
    total_id_shipping = None
    total_name_shipping = None
    total_value_shipping = None
    total_id_tax = None
    total_name_tax = None
    total_value_tax = None
    total_id_change = None
    total_name_change = None
    total_value_change = None

    '''
    Dimensiones ITEMS
    '''

    items_uniqueId = None
    items_id = None
    items_productId = None
    items_ean = None
    items_lockId = None
    item_quantity = None
    item_seller = None
    item_name = None
    item_refId = None
    item_price = None
    item_listPrice = None
    item_manualPrice = None
    item_imageUrl = None
    item_detailUrl = None
    item_sellerSku = None
    item_priceValidUntil = None
    item_commission = None
    item_tax = None
    item_preSaleDate = None
    item_itemAttachment_name = None
    item_measurementUnit = None
    item_unitMultiplier = None
    item_sellingPrice = None
    item_isGift = None
    item_shippingPrice = None
    item_rewardValue = None
    item_freightCommission = None
    item_taxCode = None
    item_parentItemIndex = None
    item_parentAssemblyBinding = None
    
    '''
    Dimensiones ITEMS_INFORMATION__ADITIONAL
    '''
    brandName = None
    brandId = None
    categoriesIds = None
    productClusterId = None
    commercialConditionId = None
    offeringInfo = None
    offeringType = None
    offeringTypeId = None
    '''
    Dimensiones ITEMS_INFORMATION__ADITIONAL_dimension
    '''
    cubicweight = None
    height = None
    length = None
    weight = None
    width = None
    '''
    Dimensiones ITEMS_priceDefinition
    '''
    total = None
    
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
        
        '''
        INIT TREE
        '''
        
        Total = Fjson["totals"]
        items = Fjson["items"]
        Items = items[0]
        itemAttachment = Items["itemAttachment"]
        additionalInfo = Items["additionalInfo"]
        priceDefinition = Items["priceDefinition"]
        sellingPrice = Items["sellingPrice"]
        dimension = additionalInfo["dimension"]
        '''
        END
        '''
        
        if Total:
            try:
                if Total[0]:
                    items = Total[0]
                    init.total_id_items = items["id"]
                    init.total_name_items = items["name"]
                    init.total_value_items = items["value"]
            except:
                print("No hay datos items")
            try:
                if Total[1]:
                    discounts = Total[1]
                    init.total_id_discounts = discounts["id"]
                    init.total_name_discounts = discounts["name"]
                    init.total_value_discounts = discounts["value"]
            except:
                print("No hay datos discounts")
            try:
                if Total[2]:
                    shipping = Total[2]
                    init.total_id_shipping = shipping["id"]
                    init.total_name_shipping = shipping["name"]
                    init.total_value_shipping = shipping["value"]
            except:
                print("No hay datos shipping")
            try:
                if Total[3]:
                    tax = Total[3]
                    init.total_id_tax = tax["id"]
                    init.total_name_tax = tax["name"]
                    init.total_value_tax = tax["value"]
            except:
                print("No hay datos tax")
            try:
                if  Total[4]:
                    change = Total[4]
                    init.total_id_change = change["id"]
                    init.total_name_change = change["name"]
                    init.total_value_change = change["value"]
            except:
                print("No hay datos change")
        if Items:
            try:
                init.items_uniqueId = Items["uniqueId"]
                init.items_id = Items["id"]
                init.items_productId = Items["productId"]
                init.items_ean = Items["ean"]
                init.items_lockId = Items["lockId"]
                init.item_quantity = Items["quantity"]
                init.item_seller = Items["seller"]
                init.item_name = Items["name"]
                init.item_refId = Items["refId"]
                init.item_price = Items["price"]
                init.item_listPrice = Items["listPrice"]
                init.item_manualPrice = Items["manualPrice"]
                init.item_imageUrl = Items["imageUrl"]
                init.item_detailUrl = Items["detailUrl"]
                init.item_sellerSku = Items["sellerSku"]
                init.item_priceValidUntil = Items["priceValidUntil"]
                init.item_commission = Items["commission"]
                init.item_tax = Items["tax"]
                init.item_preSaleDate = Items["preSaleDate"]
                init.item_measurementUnit = Items["measurementUnit"]
                init.item_unitMultiplier = Items["unitMultiplier"]
                init.item_sellingPrice = Items["sellingPrice"]
                init.item_isGift = Items["isGift"]
                init.item_shippingPrice = Items["shippingPrice"]
                init.item_rewardValue = Items["rewardValue"]
                init.item_freightCommission = Items["freightCommission"]
                init.item_taxCode = Items["taxCode"]
                init.item_parentItemIndex = Items["parentItemIndex"]
                init.item_parentAssemblyBinding = Items["parentAssemblyBinding"]
                
                '''
                Informacion Adicional
                '''
                
                init.brandName = additionalInfo["brandName"]
                init.brandId = additionalInfo["brandId"]
                init.categoriesIds = additionalInfo["categoriesIds"]
                init.productClusterId = additionalInfo["productClusterId"]
                init.commercialConditionId = additionalInfo["commercialConditionId"]
                init.offeringInfo = additionalInfo["offeringInfo"]
                init.offeringType = additionalInfo["offeringType"]
                init.offeringTypeId = additionalInfo["offeringTypeId"]
                
                '''
                información Adicional dimension
                '''
                
                init.cubicweight = dimension["cubicweight"]
                init.height = dimension["height"]
                init.length = dimension["length"]
                init.weight = dimension["weight"]
                init.width = dimension["width"]

                if Items["itemAttachment"]:
                    init.item_itemAttachment_name = itemAttachment["name"]
                
                '''    
                priceDefinition
                '''
                init.total = Items["sellingPrice"]
            except:
                print("No hay datos ITEMS")
            
            
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
            'total_id_items': init.total_id_items,
            'total_name_items': init.total_name_items,
            'total_value_items': init.total_value_items,
            'total_id_discounts': init.total_id_discounts,
            'total_name_discounts': init.total_name_discounts,
            'total_value_discounts': init.total_value_discounts,
            'total_id_shipping': init.total_id_shipping,
            'total_name_shipping': init.total_name_shipping,
            'total_value_shipping': init.total_value_shipping,
            'total_id_tax': init.total_id_tax,
            'total_name_tax': init.total_name_tax,
            'total_value_tax': init.total_value_tax,
            'total_id_change': init.total_id_change,
            'total_name_change': init.total_name_change,
            'total_value_change': init.total_value_change,
            'items_uniqueId': init.items_uniqueId,
            'items_id': init.items_id,
            'items_productId': init.items_productId,
            'items_ean': init.items_ean,
            'items_lockId': init.items_lockId,
            'item_quantity': init.item_quantity,
            'item_seller': init.item_seller,
            'item_name': init.item_name,
            'item_refId': init.item_refId,
            'item_price': init.item_price,
            'item_listPrice': init.item_listPrice,
            'item_manualPrice': init.item_manualPrice,
            'item_imageUrl': init.item_imageUrl,
            'item_detailUrl': init.item_detailUrl,
            'item_sellerSku': init.item_sellerSku,
            'item_priceValidUntil': init.item_priceValidUntil,
            'item_commission': init.item_commission,
            'item_tax': init.item_tax,
            'item_preSaleDate': init.item_preSaleDate,
            'item_measurementUnit': init.item_measurementUnit,
            'item_unitMultiplier': init.item_unitMultiplier,
            'item_sellingPrice': init.item_sellingPrice,
            'item_isGift': init.item_isGift,
            'item_shippingPrice': init.item_shippingPrice,
            'item_rewardValue': init.item_rewardValue,
            'item_freightCommission': init.item_freightCommission,
            'item_taxCode': init.item_taxCode,
            'item_parentItemIndex': init.item_parentItemIndex,
            'item_parentAssemblyBinding': init.item_parentAssemblyBinding,
            'item_itemAttachment_name': init.item_itemAttachment_name,
            'brandName': init.brandName,
            'brandId': init.brandId,
            'categoriesIds': init.categoriesIds,
            'productClusterId': init.productClusterId,
            'commercialConditionId': init.commercialConditionId,
            'offeringInfo': init.offeringInfo,
            'offeringType': init.offeringType,
            'offeringTypeId': init.offeringTypeId,
            'cubicweight': init.cubicweight,
            'height': init.height,
            'length': init.length,
            'weight': init.weight,
            'width': init.width,
            'total': init.total,
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



def run():
    #try:
    get_params()
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
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
    #except:
    #    print("Error")
    
run()