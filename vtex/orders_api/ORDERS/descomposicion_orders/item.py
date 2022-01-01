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
    df_1 = pd.DataFrame()
    df_2 = pd.DataFrame()
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema


def get_order(id):
    url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
    response = requests.request("GET", url, headers=init.headers)
    Fjson = json.loads(response.text)
    if Fjson:
        items = Fjson["items"]
        for x in items:
            items_uniqueId = x["uniqueId"]
            items_id = x["id"]
            items_productId = x["productId"]
            items_ean = str(x["ean"])
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
            if x["additionalInfo"]:
                additionalInfo = x["additionalInfo"]
                brandName = additionalInfo["brandName"]
                brandId = additionalInfo["brandId"]
                categoriesIds = additionalInfo["categoriesIds"]
                productClusterId = additionalInfo["productClusterId"]
                commercialConditionId = additionalInfo["commercialConditionId"]
                offeringInfo = additionalInfo["offeringInfo"]
                offeringType = additionalInfo["offeringType"]
                offeringTypeId = additionalInfo["offeringTypeId"]
                if additionalInfo["dimension"]:
                    dimension = additionalInfo["dimension"]
                    cubicweight = dimension["cubicweight"]
                    height = dimension["height"]
                    length = dimension["length"]
                    weight = dimension["weight"]
                    width = dimension["width"]
                    
            if x["itemAttachment"]:
                itemAttachment = x["itemAttachment"]
                item_itemAttachment_name = itemAttachment["name"]
            
        
    for y in items:
        df1 = pd.DataFrame({
            'orderId': id,
            'uniqueId': items_uniqueId,
            'id': items_id,
            'productId': items_productId,
            'ean': str(items_ean),
            'lockId': items_lockId,
            'item_quantity': item_quantity,
            'seller': item_seller,
            'name': item_name,
            'refId': item_refId,
            'item_price': item_price,
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
            'item_unitMultiplier': item_unitMultiplier,
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
            'width': width,
            'item_itemAttachment_name': item_itemAttachment_name}, index=[0])
        init_2.df = init_2.df.append(df1)
        
def get_order_package(id):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        if Fjson:
            if Fjson["packageAttachment"]:
                packageAttachment = Fjson["packageAttachment"]
                if packageAttachment["packages"]:
                    
                    packageAttachment = Fjson["packageAttachment"]
                    packages = packageAttachment["packages"]
                    
                    for x in packages:
                        package_items = packages[0]
                        items = package_items["items"]
                        courier = x["courier"]
                        invoiceNumber = x["invoiceNumber"]
                        invoiceValue = x["invoiceValue"]
                        invoiceUrl = x["invoiceUrl"]
                        issuanceDate = x["issuanceDate"]
                        trackingNumber = x["trackingNumber"]
                        invoiceKey = x["invoiceKey"]
                        trackingUrl = x["trackingUrl"]
                        embeddedInvoice = x["embeddedInvoice"]
                        package_type = x["type"]
                        cfop = x["cfop"]
                        volumes = x["volumes"]
                        EnableInferItems = x["EnableInferItems"]
                        
                        for y in items:
                            itemIndex = str(y["itemIndex"])
                            package_quantity = y["quantity"]
                            price = y["price"]
                            description = y["description"]
                            unitMultiplier = y["unitMultiplier"]
                            df2 = pd.DataFrame({
                                'courier': courier,
                                'invoiceNumber': invoiceNumber,
                                'invoiceValue': invoiceValue,
                                'invoiceUrl': invoiceUrl,
                                'issuanceDate': issuanceDate,
                                'trackingNumber': trackingNumber,
                                'invoiceKey': invoiceKey,
                                'trackingUrl': trackingUrl,
                                'embeddedInvoice': embeddedInvoice,
                                'package_type': package_type,
                                "cfop":cfop,
                                "volumes":volumes,
                                "EnableInferItems":EnableInferItems,
                                'itemIndex': str(itemIndex),
                                'package_quantity': package_quantity,
                                'package_price': price,
                                'description': description,
                                'unitMultiplier': unitMultiplier}, index=[1])
                            init.df_1 = init.df_1.append(df2)
    except:
        print("no package")

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_order_items_package` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_order_items_package`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")


def run():
    
    frames = [init.df_1, init.df_2]
    df = pd.concat(frames)
    
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
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "item_price_definition",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "rewardValue",
        "type": "FLOAT",
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
        "type": "FLOAT",
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
        "type": "FLOAT",
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
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "productId",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "item_unitMultiplier",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "item_price",
        "type": "FLOAT",
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
        "name": "item_quantity",
        "type": "FLOAT",
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
        "type": "FLOAT",
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
    },{
        "name": "courier",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "invoiceNumber",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "invoiceValue",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "invoiceUrl",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "issuanceDate",
        "type": "TIMESTAMP",
        "mode": "NULLABLE"
    },{
        "name": "trackingNumber",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "invoiceKey",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "trackingUrl",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "embeddedInvoice",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "package_type",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "cfop",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "volumes",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "EnableInferItems",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "itemIndex",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "package_quantity",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "package_price",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "description",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "unitMultiplier",
        "type": "FLOAT",
        "mode": "NULLABLE"
    }]
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_order_items_package'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate() 
    
    #try:
    #    client  = bigquery.Client(project = project_id)
    #    dataset  = client.dataset(dataset_id)
    #    table = dataset.table(table_id)
    #    job_config = bigquery.LoadJobConfig()
    #    #job_config.write_disposition = "WRITE_TRUNCATE"
    #    job_config.schema = format_schema(table_schema)
    #    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    #    job = client.load_table_from_json(json_object, table, job_config = job_config)
    #    print(job.result())
    #    delete_duplicate()
    #except:
    


def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.test.shopstar_order_items_package`))')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        print(row.orderId)
        get_order(row.orderId)
        get_order_package(row.orderId)
        print("Registro: "+str(registro))
        if registro == 5:
            run()
        if registro == 10:
            run()
        if registro == 100:
            run()
        if registro == 200:
            run()
        if registro == 500:
            run()
        if registro == 10000:
            run()
        if registro == 12000:
            run()
        if registro == 15000:
            run()
        if registro == 20000:
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
        if registro == 60000:
            run()
        if registro == 70000:
            run()
        if registro == 80000:
            run()
        if registro == 85000:
            run()
        if registro == 90000:
            run()
        if registro == 95000:
            run()
    run()
    
get_params()