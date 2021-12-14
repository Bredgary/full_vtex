import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join

class init:
    '''
    Dimensiones ITEMS
    '''
    df = pd.DataFrame()
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
    item_price_definition = None
    item_serialNumbers = None
    
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
    calculatedSellingPrice = None
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}



def get_order(id,reg):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        items = Fjson["items"]
        Items = items[0]
        additionalInfo = Items["additionalInfo"]
        dimension = additionalInfo["dimension"]
        
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
        init.item_price_definition = Items["priceDefinition"]
        init.item_serialNumbers = Items["serialNumbers"]
        init.brandName = additionalInfo["brandName"]
        init.brandId = additionalInfo["brandId"]
        init.categoriesIds = additionalInfo["categoriesIds"]
        init.productClusterId = additionalInfo["productClusterId"]
        init.commercialConditionId = additionalInfo["commercialConditionId"]
        init.offeringInfo = additionalInfo["offeringInfo"]
        init.offeringType = additionalInfo["offeringType"]
        init.offeringTypeId = additionalInfo["offeringTypeId"]
        init.cubicweight = dimension["cubicweight"]
        init.height = dimension["height"]
        init.length = dimension["length"]
        init.weight = dimension["weight"]
        init.width = dimension["width"]
        #except:
         #   print("No hay datos. dimension")
        
    
        
        df1 = pd.DataFrame({
            'orderId': id,
            'uniqueId': init.items_uniqueId,
            'items_id': init.items_id,
            'productId': init.items_productId,
            'ean': init.items_ean,
            'lockId': init.items_lockId,
            'quantity': init.item_quantity,
            'seller': init.item_seller,
            'name': init.item_name,
            'refId': init.item_refId,
            'price': init.item_price,
            'listPrice': init.item_listPrice,
            'manualPrice': init.item_manualPrice,
            'imageUrl': init.item_imageUrl,
            'detailUrl': init.item_detailUrl,
            'sellerSku': init.item_sellerSku,
            'priceValidUntil': init.item_priceValidUntil,
            'commission': init.item_commission,
            'tax': init.item_tax,
            'preSaleDate': init.item_preSaleDate,
            'measurementUnit': init.item_measurementUnit,
            'unitMultiplier': init.item_unitMultiplier,
            'sellingPrice': init.item_sellingPrice,
            'isGift': init.item_isGift,
            'shippingPrice': init.item_shippingPrice,
            'rewardValue': init.item_rewardValue,
            'freightCommission': init.item_freightCommission,
            'priceDefinition': init.item_price_definition,
            'taxCode': init.item_taxCode,
            'parentItemIndex': init.item_parentItemIndex,
            'parentAssemblyBinding': init.item_parentAssemblyBinding,
            'itemAttachment_name': init.item_itemAttachment_name,
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
            'calculatedSellingPrice': init.calculatedSellingPrice,
            'priceDefinition_total': init.total}, index=[0])
        init.df = init.df.append(df1)
        print("Registro: "+str(reg))
    except:
        print("vacio")

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
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_order_items` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_order_items`')
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
    table_id = 'shopstar_order_items'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()

run()