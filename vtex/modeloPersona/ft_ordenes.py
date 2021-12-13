import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join

class init:
    df = pd.DataFrame()

def get_params():
    try:
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.ft_ordenes` AS SELECT orderId,taxData,itemMetadata_DetailUrl,itemMetadata_Ean,itemMetadata_Id,itemMetadata_ImageUrl,itemMetadata_Name,itemMetadata_ProductId,itemMetadata_RefId,itemMetadata_Seller,itemMetadata_SkuName,allowEdition,allowCancellation,roundingError,hostname,subscriptionData,isCompleted,commercialConditionData,authorizedDate,status,seller_id,creationDate,giftRegistryData,callCenterOperatorData,marketplaceOrderId, changesAttachment_id,lastChange,orderGroup,value,invoicedDate,followUpEmail,affiliateId,origin,salesChannel,cancelReason,orderFormId,statusDescription,sellerOrderId,customData,merchantName,openTextField,marketplaceServicesEndpoint,sequence FROM `shopstar-datalake.staging_zone.shopstar_vtex_order`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
        delete_duplicate()
    except:
        print("Consulta SQL no ejecutada")

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.ft_ordenes` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.ft_ordenes`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

get_params()