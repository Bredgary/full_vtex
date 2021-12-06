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


     
def loads_ordenes(id,reg):
    
    df1 = pd.DataFrame({
        'orderId': init.orderId,
        'emailTracked': emailTracked,
        'approvedBy': init.approvedBy,
        'cancelledBy': init.cancelledBy,
        'cancelReason': init.cancelReason,
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
        'followUpEmail': followUpEmail,
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
        'DIM_TOTAL_id_items': init.total_id_items,
        'DIM_TOTAL_name_items': init.total_name_items,
        'DIM_TOTAL_value_items': init.total_value_items,
        'DIM_TOTAL_id_discounts': init.total_id_discounts,
        'DIM_TOTAL_name_discounts': init.total_name_discounts,
        'DIM_TOTAL_value_discounts': init.total_value_discounts,
        'DIM_TOTAL_id_shipping': init.total_id_shipping,
        'DIM_TOTAL_name_shipping': init.total_name_shipping,
        'DIM_TOTAL_value_shipping': init.total_value_shipping,
        'DIM_TOTAL_id_tax': init.total_id_tax,
        'DIM_TOTAL_name_tax': init.total_name_tax,
        'DIM_TOTAL_value_tax': init.total_value_tax,
        'DIM_TOTAL_id_change': init.total_id_change,
        'DIM_TOTAL_name_change': init.total_name_change,
        'DIM_TOTAL_value_change': init.total_value_change,
        'DIM_ITEMS_uniqueId': init.items_uniqueId,
        'DIM_ITEMS_items_id': init.items_id,
        'DIM_ITEMS_productId': init.items_productId,
        'DIM_ITEMS_ean': init.items_ean,
        'DIM_ITEMS_lockId': init.items_lockId,
        'DIM_ITEMS_quantity': init.item_quantity,
        'DIM_ITEMS_seller': init.item_seller,
        'DIM_ITEMS__name': init.item_name,
        'DIM_ITEMS_refId': init.item_refId,
        'DIM_ITEMS_price': init.item_price,
        'DIM_ITEMS_listPrice': init.item_listPrice,
        'DIM_ITEMS_manualPrice': init.item_manualPrice,
        'DIM_ITEMS_imageUrl': init.item_imageUrl,
        'DIM_ITEMS_detailUrl': init.item_detailUrl,
        'DIM_ITEMS_sellerSku': init.item_sellerSku,
        'DIM_ITEMS_priceValidUntil': init.item_priceValidUntil,
        'DIM_ITEMS_commission': init.item_commission,
        'DIM_ITEMS_tax': init.item_tax,
        'DIM_ITEM_preSaleDate': init.item_preSaleDate,
        'DIM_ITEM_measurementUnit': init.item_measurementUnit,
        'DIM_ITEM_unitMultiplier': init.item_unitMultiplier,
        'DIM_ITEM_sellingPrice': init.item_sellingPrice,
        'DIM_ITEM_isGift': init.item_isGift,
        'DIM_ITEM_shippingPrice': init.item_shippingPrice,
        'DIM_ITEM_rewardValue': init.item_rewardValue,
        'DIM_ITEM_freightCommission': init.item_freightCommission,
        'DIM_ITEM_priceDefinition': init.item_price_definition,
        'DIM_ITEM_taxCode': init.item_taxCode,
        'DIM_ITEM_parentItemIndex': init.item_parentItemIndex,
        'DIM_ITEM_parentAssemblyBinding': init.item_parentAssemblyBinding,
        'DIM_ITEM_itemAttachment_name': init.item_itemAttachment_name,
        'DIM_ITEM_AInfo_brandName': init.brandName,
        'DIM_ITEM_AInfo_brandId': init.brandId,
        'DIM_ITEM_AInfo_categoriesIds': init.categoriesIds,
        'DIM_ITEM_AInfo_productClusterId': init.productClusterId,
        'DIM_ITEM_AInfo_commercialConditionId': init.commercialConditionId,
        'DIM_ITEM_AInfo_offeringInfo': init.offeringInfo,
        'DIM_ITEM_AInfo_offeringType': init.offeringType,
        'DIM_ITEM_AInfo_offeringTypeId': init.offeringTypeId,
        'DIM_ITEM_AInfo_cubicweight': init.cubicweight,
        'DIM_ITEM_AInfo_dim_height': init.height,
        'DIM_ITEM_AInfo_dim_length': init.length,
        'DIM_ITEM_AInfo_dim_weight': init.weight,
        'DIM_ITEM_AInfo_dim_width': init.width,
        'DIM_ITEM_calculatedSellingPrice': init.calculatedSellingPrice,
        'DIM_ITEM_priceDefinition_total': init.total,
        'DIM_CLIENT': init.client_id,
        'DIM_CLIENT_email': client_email,
        'DIM_CLIENT_firstName': init.client_firstName,
        'DIM_CLIENT_lastName': init.client_lastName,
        'DIM_CLIENT_documentType': init.client_documentType,
        'DIM_CLIENT_document': init.client_document,
        'DIM_CLIENT_phone': init.client_phone,
        'DIM_CLIENT_corporateName': init.client_corporateName,
        'DIM_CLIENT_tradeName': init.client_tradeName,
        'DIM_CLIENT_corporateDocument': init.client_corporateDocument,
        'DIM_CLIENT_stateInscription': init.client_stateInscription,
        'DIM_CLIENT_corporatePhone': init.client_corporatePhone,
        'DIM_CLIENT_isCorporate': init.client_isCorporate,
        'DIM_CLIENT_userProfileId': init.client_userProfileId,
        'DIM_CLIENT_customerClass': init.client_customerClass,
        'id_ratesAndBenefits': init.id_ratesAndBenefits,
        'DIM_SHIPPING_DATA_shippingData_id': init.shippingData_id,
        'DIM_SHIPPING_DATA_addressType': init.addressType,
        'DIM_SHIPPING_DATA_receiverName': init.receiverName,
        'DIM_SHIPPING_DATA_addressId': init.addressId,
        'DIM_SHIPPING_DATA_postalCode': init.postalCode,
        'DIM_SHIPPING_DATA_city': init.city,
        'DIM_SHIPPING_DATA_state': init.state,
        'DIM_SHIPPING_DATA_country': init.country,
        'DIM_SHIPPING_DATA_street': init.street,
        'DIM_SHIPPING_DATA_number': init.number,
        'DIM_SHIPPING_DATA_neighborhood': init.neighborhood,
        'DIM_SHIPPING_DATA_complement': init.complement,
        'DIM_SHIPPING_DATA_reference': init.reference,
        'DIM_SHIPPING_DATA_trackingHints': init.trackingHints,
        'DIM_SHIPPING_DATA_deliveryChannel': init.deliveryChannel,
        'DIM_SHIPPING_DATA_addressId': init.addressId,
        'DIM_SHIPPING_DATA_polygonName': init.polygonName,
        'DIM_SHIPPING_DATA_itemIndex': init.itemIndex,
        'DIM_SHIPPING_DATA_selectedSla': init.selectedSla,
        'DIM_SHIPPING_DATA_lockTTL': init.lockTTL,
        'DIM_SHIPPING_DATA_price': init.price,
        'DIM_SHIPPING_DATA_listPrice': init.listPrice,
        'DIM_SHIPPING_DATA_sellingPrice': init.sellingPrice,
        'DIM_SHIPPING_DATA_deliveryWindow': init.deliveryWindow,
        'DIM_SHIPPING_DATA_deliveryCompany': init.deliveryCompany,
        'DIM_SHIPPING_DATA_shippingEstimate': init.shippingEstimate,
        'DIM_SHIPPING_DATA_shippingEstimateDate': init.shippingEstimateDate,
        'DIM_SHIPPING_DATA_slas_id': init.slas_id,
        'DIM_SHIPPING_DATA_slas_name': init.slas_name,
        'DIM_SHIPPING_DATA_slas_shippingEstimate': init.slas_shippingEstimate,
        'DIM_SHIPPING_DATA_slas_deliveryWindow': init.slas_deliveryWindow,
        'DIM_SHIPPING_DATA_slas_price': init.slas_price,
        'DIM_SHIPPING_DATA_slas_deliveryChannel': init.slas_deliveryChannel,
        'DIM_SHIPPING_DATA_slas_polygonName': init.slas_polygonName,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo': init.slas_pickupStoreInfo_additionalInfo,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_address': init.slas_pickupStoreInfo_address,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId': init.slas_pickupStoreInfo_dockId,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName': init.slas_pickupStoreInfo_friendlyName,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore': init.slas_pickupStoreInfo_isPickupStore,
        'DIM_SHIPPING_DATA_slas_id_01': init.slas_id_01,
        'DIM_SHIPPING_DATA_slas_name_01': init.slas_name_01,
        'DIM_SHIPPING_DATA_slas_shippingEstimate_01': init.slas_shippingEstimate_01,
        'DIM_SHIPPING_DATA_slas_deliveryWindow_01': init.slas_deliveryWindow_01,
        'DIM_SHIPPING_DATA_slas_price_01': init.slas_price_01,
        'DIM_SHIPPING_DATA_slas_deliveryChannel_01': init.slas_deliveryChannel_01,
        'DIM_SHIPPING_DATA_slas_polygonName_01': init.slas_polygonName_01,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_01': init.slas_pickupStoreInfo_additionalInfo_01,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_address_01': init.slas_pickupStoreInfo_address_01,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_01': init.slas_pickupStoreInfo_dockId_01,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_01': init.slas_pickupStoreInfo_friendlyName_01,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_01': init.slas_pickupStoreInfo_isPickupStore_01,
        'DIM_SHIPPING_DATA_slas_id_02': init.slas_id_02,
        'DIM_SHIPPING_DATA_slas_name_02': init.slas_name_02,
        'DIM_SHIPPING_DATA_slas_shippingEstimate_02': init.slas_shippingEstimate_02,
        'DIM_SHIPPING_DATA_slas_deliveryWindow_02': init.slas_deliveryWindow_02,
        'DIM_SHIPPING_DATA_slas_price_02': init.slas_price_02,
        'DIM_SHIPPING_DATA_slas_deliveryChannel_02': init.slas_deliveryChannel_02,
        'DIM_SHIPPING_DATA_slas_polygonName_02': init.slas_polygonName_02,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_02': init.slas_pickupStoreInfo_additionalInfo_02,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_address_02': init.slas_pickupStoreInfo_address_02,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_02': init.slas_pickupStoreInfo_dockId_02,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_02': init.slas_pickupStoreInfo_friendlyName_02,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_02': init.slas_pickupStoreInfo_isPickupStore_02,
        'DIM_SHIPPING_DATA_slas_id_03': init.slas_id_03,
        'DIM_SHIPPING_DATA_slas_name_03': init.slas_name_03,
        'DIM_SHIPPING_DATA_slas_shippingEstimate_03': init.slas_shippingEstimate_03,
        'DIM_SHIPPING_DATA_slas_deliveryWindow_03': init.slas_deliveryWindow_03,
        'DIM_SHIPPING_DATA_slas_price_03': init.slas_price_03,
        'DIM_SHIPPING_DATA_slas_deliveryChannel_03': init.slas_deliveryChannel_03,
        'DIM_SHIPPING_DATA_slas_polygonName_03': init.slas_polygonName_03,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_03': init.slas_pickupStoreInfo_additionalInfo_03,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_address_03': init.slas_pickupStoreInfo_address_03,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_03': init.slas_pickupStoreInfo_dockId_03,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_03': init.slas_pickupStoreInfo_friendlyName_03,
        'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_03': init.slas_pickupStoreInfo_isPickupStore_03,
        'DIM_SHIPPING_DATA_courierId_delivery': init.courierId,
        'DIM_SHIPPING_DATA_courierName_delivery': init.courierName,
        'DIM_SHIPPING_DATA_dockId_delivery': init.dockId,
        'DIM_SHIPPING_DATA_quantity_delivery': init.quantity,
        'DIM_SHIPPING_DATA_warehouseId': init.warehouseId,
        'DIM_SHIPPING_DATA_pickupStoreInfo_additionalInfo': init.pickupStoreInfo_additionalInfo,
        'DIM_SHIPPING_DATA_pickupStoreInfo_address': init.pickupStoreInfo_address,
        'DIM_SHIPPING_DATA_pickupStoreInfo_dockId': init.pickupStoreInfo_dockId,
        'DIM_SHIPPING_DATA_pickupStoreInfo_friendlyName': init.pickupStoreInfo_friendlyName,
        'DIM_SHIPPING_DATA_pickupStoreInfo_isPickupStore': init.pickupStoreInfo_isPickupStore,
        'DIM_SHIPPING_DATA_selectedAddresses_addressId': init.selectedAddresses_addressId,
        'DIM_SHIPPING_DATA_selectedAddresses_addressType': init.selectedAddresses_addressType,
        'DIM_SHIPPING_DATA_selectedAddresses_receiverName': init.selectedAddresses_receiverName,
        'DIM_SHIPPING_DATA_selectedAddresses_street': init.selectedAddresses_street,
        'DIM_SHIPPING_DATA_selectedAddresses_number': init.selectedAddresses_number,
        'DIM_SHIPPING_DATA_selectedAddresses_complement': init.selectedAddresses_complement,
        'DIM_SHIPPING_DATA_selectedAddresses_neighborhood': init.selectedAddresses_neighborhood,
        'DIM_SHIPPING_DATA_selectedAddresses_postalCode': init.selectedAddresses_postalCode,
        'DIM_SHIPPING_DATA_selectedAddresses_city': init.selectedAddresses_city,
        'DIM_SHIPPING_DATA_selectedAddresses_state': init.selectedAddresses_state,
        'DIM_SHIPPING_DATA_selectedAddresses_country': init.selectedAddresses_country,
        'DIM_SHIPPING_DATA_selectedAddresses_reference': init.selectedAddresses_reference,
        'transactions_isActive': init.transactions_isActive,
        'transactions_transactionId': init.transactions_transactionId,
        'transactions_merchantName': init.transactions_merchantName,
        'payments_id': init.payments_id,
        'payments_paymentSystem': init.payments_paymentSystem,
        'payments_paymentSystemName': init.payments_paymentSystemName,
        'payments_value': init.payments_value,
        'payments_installments': init.payments_installments,
        'payments_referenceValue': init.payments_referenceValue,
        'payments_cardHolder': init.payments_cardHolder,
        'payments_firstDigits': init.payments_firstDigits,
        'payments_lastDigits': init.payments_lastDigits,
        'payments_url': init.payments_url,
        'payments_giftCardId': init.payments_giftCardId,
        'payments_giftCardName': init.payments_giftCardName,
        'payments_giftCardCaption': init.payments_giftCardCaption,
        'payments_redemptionCode': init.payments_redemptionCode,
        'payments_group': init.payments_group,
        'payments_tid': init.payments_tid,
        'payments_dueDate': init.payments_dueDate,
        'billingAddress_postalCode': init.billingAddress_postalCode,
        'billingAddress_city': init.billingAddress_city,
        'billingAddress_state': init.billingAddress_state,
        'billingAddress_country': init.billingAddress_country,
        'billingAddress_street': init.billingAddress_street,
        'billingAddress_number': init.billingAddress_number,
        'billingAddress_neighborhood': init.billingAddress_neighborhood,
        'billingAddress_complement': init.billingAddress_complement,
        'billingAddress_reference': init.billingAddress_reference,
        'seller_id': init.seller_id,
        'seller_name': init.seller_name,
        'seller_logo': init.seller_logo,
        'changesAttachment_id': init.changesAttachment_id,
        'storePreferencesData_countryCode': init.storePreferencesData_countryCode,
        'storePreferencesData_currencyCode': init.storePreferencesData_currencyCode,
        'storePreferencesData_currencyLocale': init.storePreferencesData_currencyLocale,
        'storePreferencesData_currencySymbol': init.storePreferencesData_currencySymbol,
        'storePreferencesData_timeZone': init.storePreferencesData_timeZone,
        'CurrencyDecimalDigits': init.CurrencyDecimalDigits,
        'CurrencyDecimalSeparator': init.CurrencyDecimalSeparator,
        'CurrencyGroupSeparator': init.CurrencyGroupSeparator,
        'CurrencyGroupSize': init.CurrencyGroupSize,
        'StartsWithCurrencySymbol': init.StartsWithCurrencySymbol,
        'marketplace_baseURL': init.baseURL,
        'marketplace_isCertified': init.isCertified,
        'marketplace_name': init.name,
        'itemMetadata_Id': init.itemMetadata_Id,
        'itemMetadata_Seller': init.itemMetadata_Seller,
        'itemMetadata_Name': init.itemMetadata_Name,
        'itemMetadata_SkuName': init.itemMetadata_SkuName,
        'itemMetadata_ProductId': init.itemMetadata_ProductId,
        'itemMetadata_RefId': init.itemMetadata_RefId,
        'itemMetadata_Ean': init.itemMetadata_Ean,
        'itemMetadata_ImageUrl': init.itemMetadata_ImageUrl,
        'itemMetadata_DetailUrl': init.itemMetadata_DetailUrl,
        'invoicedDate': init.invoicedDate}, index=[0])
    init.df = init.df.append(df1)
    print("Registro: "+str(reg))
        

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_vtex_order`')
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
		QUERY = (
			'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.ft_ordenes` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.ft_ordenes`')
		query_job = client.query(QUERY)
		rows = query_job.result()
		print(rows)
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