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


def get_params():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = ("SELECT orderId,'pendiente' as id_customer,taxData,itemMetadata_DetailUrl,itemMetadata_Ean,itemMetadata_Id,itemMetadata_ImageUrl,itemMetadata_Name,itemMetadata_ProductId,itemMetadata_RefId,itemMetadata_Seller,itemMetadata_SkuName,allowEdition,allowCancellation,roundingError,hostname,subscriptionData,isCompleted,commercialConditionData,authorizedDate,status,seller_id,marketplaceOrderId,creationDate,giftRegistryData,callCenterOperatorData,'pendiente' as id_user,'pendiente' as id_item,marketplaceOrderIdchangesAttachment_id,lastChange,'pendiente' as id_marketplace,orderGroup,'pendiente' quantity,value,invoicedDate,followUpEmail,'pendiente' as id_package,affiliateId,origin,salesChannel,cancelReason,'pendiente' as checkeInPickupPointId,orderFormId,statusDescription,sellerOrderId,customData,merchantName,openTextField,marketplaceServicesEndpoint,'pendiente' as id_addressShipping,sequenceFROM `shopstar-datalake.staging_zone.shopstar_vtex_order`")
        query_job = client.query(QUERY)
        rows = query_job.result()
        for row in rows:
            df1 = pd.DataFrame({
                'orderId': row.orderId,
                'id_customer': row.id_customer,
                'taxData': row.taxData,
                'itemMetadata_DetailUrl': row.itemMetadata_DetailUrl,
                'itemMetadata_Ean': row.itemMetadata_Ean,
                'itemMetadata_Id': row.itemMetadata_Id,
                'itemMetadata_ImageUrl': row.itemMetadata_ImageUrl,
                'itemMetadata_Name': row.itemMetadata_Name,
                'itemMetadata_ProductId': row.itemMetadata_ProductId,
                'itemMetadata_RefId': row.itemMetadata_RefId,
                'itemMetadata_Seller': row.itemMetadata_Seller,
                'itemMetadata_SkuName': row.itemMetadata_SkuName,
                'allowEdition': row.allowEdition,
                'allowCancellation': row.allowCancellation,
                'roundingError': row.roundingError,
                'hostname': row.hostname,
                'subscriptionData': row.subscriptionData,
                'isCompleted': row.isCompleted,
                'commercialConditionData': row.commercialConditionData,
                'authorizedDate': row.authorizedDate,
                'status': row.status,
                'seller_id': row.seller_id,
                'marketplaceOrderId': row.marketplaceOrderId,
                'creationDate': row.creationDate,
                'giftRegistryData': row.giftRegistryData,
                'callCenterOperatorData': row.callCenterOperatorData,
                'id_user': row.id_user,
                'id_item': row.id_item,
                'changesAttachment_id': row.changesAttachment_id,
                'lastChange': row.lastChange,
                'id_marketplace': row.id_marketplace,
                'orderGroup': row.orderGroup,
                'quantity': row.quantity,
                'value': row.value,
                'invoicedDate': row.invoicedDate,
                'followUpEmail': row.followUpEmail,
                'id_package': row.id_package,
                'affiliateId': row.affiliateId,
                'origin': row.origin,
                'salesChannel': row.salesChannel,
                'cancelReason': row.cancelReason,
                'checkeInPickupPointId': row.checkeInPickupPointId,
                'orderFormId': row.orderFormId,
                'statusDescription': row.statusDescription,
                'sellerOrderId': row.sellerOrderId,
                'customData': row.customData,
                'merchantName': row.merchantName,
                'openTextField': row.openTextField,
                'marketplaceServicesEndpoint': row.marketplaceServicesEndpoint,
                'id_addressShipping': row.id_addressShipping,
                'sequence': row.sequence}, index=[0])
            init.df = init.df.append(df1)
    except:
        print("Consulta SQL no ejecutada")



def run():
    try:
        get_params()
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        project_id = '999847639598'
        dataset_id = 'cons_zone'
        table_id = 'ft_ordenes'
        
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
        print("Error")
    
run()