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

def get_order(id):
    try:
        df1 = pd.DataFrame()
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        lastChange = Fjson["lastChange"]
        items = Fjson["items"]
        for x in items:
            items_uniqueId = x["uniqueId"]
            items_id = x["id"]
            items_productId = x["productId"]
            ean = x["ean"]
            items_lockId = x["lockId"]
            item_quantity = x["quantity"]
            item_seller = x["seller"]
            item_name = x["name"]
            refId = x["refId"]
            item_price = x["price"]
            item_listPrice = x["listPrice"]
            item_manualPrice = x["manualPrice"]
            item_imageUrl = x["imageUrl"]
            item_detailUrl = x["detailUrl"]
            item_sellerSku = x["sellerSku"]
            print(item_sellerSku)
            if x["priceValidUntil"] is not None:
                item_priceValidUntil = x["priceValidUntil"]
            else:
                item_priceValidUntil = '1900-01-01 15:15:18.051893 UTC'
            
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
            'ean': str("ean: "+str(ean)),
            'lockId': items_lockId,
            'quantity': item_quantity,
            'seller': item_seller,
            'name': item_name,
            'refId': str("refId: "+str(refId)),
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
            'lastChange': lastChange,
            'height': height,
            'length': length,
            'weight': weight,
            'width': width,
            'item_itemAttachment_name': item_itemAttachment_name}, index=[0])
        init.df = init.df.append(df1)
        if df1.empty:
            df1 = pd.DataFrame({
                'orderId': id}, index=[0])
            init.df = init.df.append(df1)
    except:
        print("Error.")
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
        print("Consulta SQL no ejecutada")


def run():
    try:
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        table_schema = [
            {
              "name": "length",
              "type": "FLOAT",
              "mode": "NULLABLE"
            },{
              "name": "cubicweight",
              "type": "FLOAT",
              "mode": "NULLABLE"
            },{
              "name": "offeringInfo",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "commercialConditionId",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "categoriesIds",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "brandId",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "brandName",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "parentAssemblyBinding",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "height",
              "type": "FLOAT",
              "mode": "NULLABLE"
            },{
              "name": "productClusterId",
              "type": "FLOAT",
              "mode": "NULLABLE"
            },{
              "name": "parentItemIndex",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "taxCode",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "item_itemAttachment_name",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "freightCommission",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "item_price_definition",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "rewardValue",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "width",
              "type": "FLOAT",
              "mode": "NULLABLE"
            },{
              "name": "shippingPrice",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "isGift",
              "type": "BOOLEAN",
              "mode": "NULLABLE"
            },{
              "name": "orderId",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "sellingPrice",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "name",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "measurementUnit",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "detailUrl",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "preSaleDate",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "tax",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "priceValidUntil",
              "type": "TIMESTAMP",
              "mode": "NULLABLE"
            },{
              "name": "item_serialNumbers",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "sellerSku",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "id",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "manualPrice",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "commission",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "productId",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "unitMultiplier",
              "type": "FLOAT",
              "mode": "NULLABLE"
            },{
              "name": "price",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "seller",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "lockId",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "imageUrl",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "offeringType",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "quantity",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "refId",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "ean",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "listPrice",
              "type": "INTEGER",
              "mode": "NULLABLE"
            },{
              "name": "weight",
              "type": "FLOAT",
              "mode": "NULLABLE"
            },{
              "name": "offeringTypeId",
              "type": "STRING",
              "mode": "NULLABLE"
            },{
              "name": "uniqueId",
              "type": "STRING",
              "mode": "NULLABLE"
            }]
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_order_items'
        
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
            client = bigquery.Client(project = project_id)
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
        QUERY = ('    SELECT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_order_items`))')
        query_job = client.query(QUERY)
        rows = query_job.result()
        registro = 0
        for row in rows:
            registro += 1
            get_order(row.orderId)
            print("Registro: "+str(registro))
            if registro == 10:
                run()
            if registro == 20:
                run()
            if registro == 30:
                run()
            if registro == 40:
                run()
            if registro == 50:
                run()
            if registro == 60:
                run()
            if registro == 70:
                run()
            if registro == 80:
                run()
            if registro == 90:
                run()
            if registro == 100:
                run()
        run()
    except:
        print("Error.")
        logging.exception("message")
    
get_params()
