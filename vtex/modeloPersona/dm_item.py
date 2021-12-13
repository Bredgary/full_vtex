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
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_item` AS SELECT orderId, DIM_ITEM_AInfo_categoriesIds id_category, DIM_ITEMS_name name, DIM_ITEMS_productId productId, itemMetadata_SkuName skuName, serialNumbers, DIM_ITEM_parentItemIndex parentItemIndex,DIM_ITEM_taxCode txCode, DIM_ITEM_freightCommission freightCommission,DIM_ITEM_rewardValue rewardValue,DIM_ITEM_shippingPrice shippingPrice,DIM_ITEM_sellingPrice sellingPrice,DIM_ITEMS_tax tax,DIM_ITEMS_priceValidUntil priceValidUntil, DIM_ITEMS_sellerSku sellerSku, DIM_ITEM_isGift isGift, DIM_ITEM_parentAssemblyBinding Assembly,DIM_ITEM_preSaleDate preSaleDate,DIM_ITEMS_detailUrl detailUrl,DIM_ITEM_measurementUnit measurementUnit, seller_id id_seller, DIM_ITEMS_lockId lockId, DIM_ITEMS_manualPrice manualPrice, DIM_ITEMS_refId refId,DIM_ITEMS_listPrice listPrice, callCenterOperatorData, DIM_ITEMS_uniqueId uniqueId, DIM_ITEMS_ean ean,DIM_ITEM_parentAssemblyBinding parentAssemblyBinding,DIM_ITEM_unitMultiplier unitMultiplier,DIM_ITEMS_price price,DIM_ITEMS_commission commission FROM `shopstar-datalake.staging_zone.shopstar_vtex_order`')
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
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_item` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.dm_item`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

get_params()