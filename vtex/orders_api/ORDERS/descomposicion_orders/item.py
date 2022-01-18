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

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def get_order(id, reg):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        
        items = Fjson["items"]
        
        for x in items:
            items_uniqueId = x["uniqueId"]
            items_id = x["id"]
            items_productId = x["productId"]
            items_ean = x["ean"]
            items_lockId = x["lockId"]
            item_quantity = x["quantity"]
            item_seller = x["seller"]
            item_name = x["name"]
            item_refId = x["refId"]
            item_price = x["price"]
            item_listPrice = x["listPrice"]
            item_manualPrice = x["manualPrice"]
            item_imageUrl = x["imageUrl"]
            item_detailUrl = x["detailUrl"]
            item_sellerSku = x["sellerSku"]
            item_priceValidUntil = x["priceValidUntil"]
            item_commission = x["commission"]
            item_tax = x["tax"]
            item_preSaleDate = str(x["preSaleDate"])
            item_measurementUnit = x["measurementUnit"]
            item_unitMultiplier = x["unitMultiplier"]
            item_sellingPrice = x["sellingPrice"]
            item_isGift = x["isGift"]
            item_shippingPrice = x["shippingPrice"]
            item_rewardValue = x["rewardValue"]
            item_freightCommission = x["freightCommission"]
            item_taxCode = x["taxCode"]
            item_parentItemIndex = x["parentItemIndex"]
            item_parentAssemblyBinding = x["parentAssemblyBinding"]
            item_price_definition = x["priceDefinition"]
            item_serialNumbers = x["serialNumbers"]
            try:
                additionalInfo = x["additionalInfo"]
                brandName = additionalInfo["brandName"]
                brandId = additionalInfo["brandId"]
                categoriesIds = additionalInfo["categoriesIds"]
                productClusterId = additionalInfo["productClusterId"]
                commercialConditionId = additionalInfo["commercialConditionId"]
                offeringInfo = additionalInfo["offeringInfo"]
                offeringType = additionalInfo["offeringType"]
                offeringTypeId = additionalInfo["offeringTypeId"]
            except:
                additionalInfo = ''
                brandName = ''
                brandId = ''
                categoriesIds = ''
                productClusterId = ''
                commercialConditionId = ''
                offeringInfo = ''
                offeringType = ''
                offeringTypeId = ''
            try:
                dimension = additionalInfo["dimension"]
                cubicweight = dimension["cubicweight"]
                height = dimension["height"]
                length = dimension["length"]
                weight = dimension["weight"]
                width = dimension["width"]
            except:
                cubicweight = ''
                height = ''
                length = ''
                weight = ''
                width = ''
            try:
                itemAttachment = x["itemAttachment"]
                item_itemAttachment_name = itemAttachment["name"]
            except:
                item_itemAttachment_name = ''
                
            df1 = pd.DataFrame({
                'orderId': id,
                'uniqueId': items_uniqueId,
                'id': items_id,
                'productId': items_productId,
                'ean': items_ean,
                'lockId': items_lockId,
                'quantity': item_quantity,
                'seller': item_seller,
                'name': item_name,
                'refId': item_refId,
                'price': item_price,
                'listPrice': item_listPrice,
                'manualPrice': item_manualPrice,
                'imageUrl': item_imageUrl,
                'detailUrl': item_detailUrl,
                'sellerSku': item_sellerSku,
                'priceValidUntil': item_priceValidUntil,
                'commission': item_commission,
                'tax': item_tax,
                'preSaleDate': item_preSaleDate,
                'measurementUnit': item_measurementUnit,
                'unitMultiplier': item_unitMultiplier,
                'sellingPrice': item_sellingPrice,
                'isGift': item_isGift,
                'shippingPrice': item_shippingPrice,
                'rewardValue': item_rewardValue,
                'freightCommission': item_freightCommission,
                'taxCode': item_taxCode,
                'parentItemIndex': item_parentItemIndex,
                'parentAssemblyBinding': item_parentAssemblyBinding,
                'item_price_definition': item_price_definition,
                'item_serialNumbers': item_serialNumbers,
                'brandName': brandName,
                'brandId': brandId,
                'categoriesIds': categoriesIds,
                'productClusterId': productClusterId,
                'commercialConditionId': commercialConditionId,
                'offeringInfo': offeringInfo,
                'offeringType': offeringType,
                'offeringTypeId': offeringTypeId,
                'cubicweight': cubicweight,
                'height': height,
                'length': length,
                'weight': weight,
                'width': width,'item_itemAttachment_name': item_itemAttachment_name}, index=[0])
            init.df = init.df.append(df1)
        print("Registro: "+str(reg))
    except:
        print("Error: "+str(reg))
        logging.exception("message")

def delete_duplicate():
  try:
    print("Eliminando duplicados")
    client = bigquery.Client()
    QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_order_items` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_order_items`')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print(rows)
  except:
    print("Error.")
    logging.exception("message")


def run():
    try:
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_order_items'
        
        if df.empty:
            print('DataFrame is empty!')
        else:
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
    try:
        print("Cargando consulta")
        client = bigquery.Client()
        QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_order_items`))')
        query_job = client.query(QUERY)
        rows = query_job.result()
        registro = 0
        for row in rows:
            registro += 1
            get_order(row.orderId,registro)
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
        run()
    except:
       print("Error.")
       logging.exception("message")

get_params()
