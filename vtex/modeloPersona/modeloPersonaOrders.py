import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join
import logging

def create_dm_category():
  try:
    print("Creacion dm_date")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_category` AS 
    SELECT 
    id,
    name,
    url,
    title,
    metaTagDescription,
    predecessor,
    hasChildren 
    FROM `shopstar-datalake.staging_zone.shopstar_vtex_category`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_category actualizado exitosamente")
  except:
    print("Error dm_category!!")
    logging.exception("message")


def create_dm_date_holidays():
  try:
    print("Creacion dm_date")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_date` AS 
    SELECT 
    name, 
    endDate, 
    startDate,
    account, 
    id  
    FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_all_holidays`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_date actualizado exitosamente")
  except:
    print("Error dm_date!!")
    logging.exception("message")

def create_dm_total_Discounts():
  try:
    print("Creacion dm_total_Discounts")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_total_Discounts` AS 
    SELECT
    orderId,
    totals_name_discounts name,
    totals_value_discounts value,
    FROM `shopstar-datalake.staging_zone.shopstar_ft_orders` ''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_total_Discounts actualizado exitosamente")
  except:
    print("Error dm_total_Discounts!!")
    logging.exception("message")

def create_dm_total_Items():
  try:
    print("Creacion dm_total_Items")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_total_Items` AS 
    SELECT
    orderId,
    totals_name_items name,
    totals_value_items value,
    FROM `shopstar-datalake.staging_zone.shopstar_ft_orders` ''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_total_Items actualizado exitosamente")
  except:
    print("Error dm_total_Items!!")
    logging.exception("message")
    
def create_dm_total_Tax():
  try:
    print("Creacion dm_total_Tax")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_total_Tax` AS 
    SELECT
    orderId,
    totals_name_tax name,
    totals_value_tax value,
    FROM `shopstar-datalake.staging_zone.shopstar_ft_orders` ''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_total_Tax actualizado exitosamente")
  except:
    print("Error dm_total_Tax!!")
    logging.exception("message")
    
def create_dm_total_Shipping():
  try:
    print("Creacion dm_total_Shipping")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_total_Shipping` AS 
    SELECT
    orderId,
    totals_name_shipping name,
    totals_value_shipping value,
    FROM `shopstar-datalake.staging_zone.shopstar_ft_orders` ''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_total_Shipping actualizado exitosamente")
  except:
    print("Error dm_total_Shipping!!")
    logging.exception("message")

def create_dm_package():
  try:
    print("Creacion dm_package")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_package` AS 
    SELECT 
    description,
    itemIndex,
    price,
    EnableInferItems,
    cfop,
    embeddedInvoice,
    invoiceNumber,
    invoiceKey,
    issuanceDate,
    orderId,
    invoiceValue,
    unitMultiplier,
    trackingNumber,
    quantity,
    courier,
    volumes,
    package_type,
    trackingUrl,
    invoiceUrl
    FROM `shopstar-datalake.staging_zone.shopstar_order_package`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_package actualizado exitosamente")
  except:
    print("Error dm_package!!")
    logging.exception("message")

def create_ft_cancellations():
  try:
    print("Creacion shopstar_ft_orders")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.ft_cancellations` AS 
    SELECT GENERATE_UUID() id_cancellations,
    orderId,
    cancellationDate,
    cancelReason, 
    creationDate 
    FROM `shopstar-datalake.staging_zone.shopstar_ft_orders` ''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("shopstar_ft_orders actualizado exitosamente")
  except:
    print("Error shopstar_ft_orders!!")
    logging.exception("message")


def create_dm_user():
  try:
    print("Creacion dm_user")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_user` AS 
    SELECT 
    id, 
    isAdmin, 
    isBlocked, 
    name,
    email, 
    isReliable 
    FROM `shopstar-datalake.staging_zone.shopstar_vtex_user` ''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_user actualizado exitosamente")
  except:
    print("Error dm_user!!")
    logging.exception("message")

def create_dm_marketplace():
  try:
    print("Creacion dm_marketplace")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_marketplace` AS 
    SELECT 
    marketplaceOrderId, 
    name, 
    isCertified, 
    baseURL 
    FROM `shopstar-datalake.staging_zone.shopstar_ft_orders`;''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_marketplace actualizado exitosamente")
  except:
    print("Error dm_marketplace!!")
    logging.exception("message")

def create_dm_seller():
  try:
    print("Creacion ft_invoices")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_seller` AS 
    SELECT  SellerId,FulfillmentEndpoint, Name, UrlLogo seller_logo FROM `shopstar-datalake.staging_zone.shopstar_vtex_seller`;''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_seller actualizado exitosamente")
  except:
    print("Error dm_seller!!")
    logging.exception("message")

def create_ft_invoices():
  try:
    print("Creacion ft_invoices")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.ft_invoices` AS 
    SELECT orderid, userPaymentInfo, invoice_address FROM `shopstar-datalake.staging_zone.shopstar_vtex_order`;''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("ft_invoices actualizado exitosamente")
  except:
    print("Error ft_invoices!!")
    logging.exception("message")

def create_dm_items():
  try:
    print("Creacion dm_item")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_item` AS 
    SELECT orderId, 
    categoriesIds, 
    name,
    productId,
    sellerSku,
    item_serialNumbers, 
    parentItemIndex,
    taxCode, 
    freightCommission,
    rewardValue,
    shippingPrice,
    sellingPrice,
    tax,
    priceValidUntil,  
    isGift, 
    parentAssemblyBinding Assembly,
    preSaleDate,
    detailUrl,
    measurementUnit, 
    seller seller_id,
    lockId lockId, 
    manualPrice, 
    refId,
    listPrice,
    uniqueId,
    ean,
    parentAssemblyBinding,
    unitMultiplier,
    price,
    commission FROM `shopstar-datalake.staging_zone.shopstar_order_items`;''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_item actualizado exitosamente")
  except:
    print("Error dm_item!!")
    logging.exception("message")

def create_ft_payments():
  try:
    print("Creacion ft_payments")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.ft_payments` AS 
    SELECT 
    id_payments id_payments,
    payments.orderId id_order,
    postalCode,
    billingAddress.city,
    billingAddress.state,
    billingAddress.country,
    billingAddress.street,
    billingAddress.number,
    payments.bankIssuedInvoiceBarCodeNumber bankIssuedInvoiceBarCodeNumber,
    payments.bankIssuedInvoiceIdentificationNumber bankIssuedInvoiceIdentificationNumber,
    payments.accountId accountId, 
    payments.parentAccountId parentAccountId, 
    payments.koinUrl koinUrl,
    payments.giftCardAsDiscount giftCardAsDiscount,
    payments.group,
    payments.dueDate dueDate,
    payments.redemptionCode redemptionCode,
    payments.giftCardProvider giftCardProvider, 
    payments.tid tid, 
    payments.giftCardCaption giftCardCaption, 
    payments.giftCardName giftCardName,
    payments.bankIssuedInvoiceIdentificationNumberFormatted bankIssuedInvoiceIdentificationNumberFormatted,
    connectorResponses.ReturnCode connectorResponses_ReturnCode,
    connectorResponses.Message connectorResponses_payments_Message,
    connectorResponses.authId connectorResponses_authId,
    payments.expireYear expireYear,
    payments.expireMonth expireMonth,
    payments.firstDigits firstDigits, 
    payments.paymentSystemName paymentSystemName,
    payments.referenceValue referenceValue, 
    payments.cardNumber cardNumber, 
    payments.lastDigits lastDigits, 
    payments.cvv2 cvv2, 
    payments.cardHolder cardHolder,
    payments.giftCardId giftCardId,
    GENERATE_UUID() id,
    payments.bankIssuedInvoiceBarCodeType bankIssuedInvoiceBarCodeType,
    payments.paymentSystem paymentSystem,
    payments.url url,
    payments.value value,
    payments.installments installments,
    transactions.merchantName merchantName, 
    transactions.transactionId transactionId, 
    transactions.isActive isActive, 
    payments.giftCardName giftCard 
    FROM `shopstar-datalake.staging_zone.shopstar_order_payments` as payments
    JOIN 
    (SELECT * FROM `shopstar-datalake.staging_zone.shopstar_order_billingAddress`) as billingAddress
    ON 
    payments.orderId = billingAddress.orderId
    JOIN 
    (SELECT * FROM `shopstar-datalake.staging_zone.shopstar_order_connectorResponses`) as connectorResponses
    ON 
    connectorResponses.orderId = billingAddress.orderId
    JOIN 
    (SELECT * FROM `shopstar-datalake.staging_zone.shopstar_order_transactions`) as transactions
    ON 
    transactions.orderId = billingAddress.orderId''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("ft_payments actualizado exitosamente")
  except:
    print("Error ft_payments!!")
    logging.exception("message")

def create_ft_orders():
  try:
    print("Creacion ft_orders")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.ft_orders` AS 
    SELECT 
    shipping_neighborhood,
    shipping_state,
    shipping_city,
    shipping_postalCode,
    shipping_addressId,
    shipping_addressType,
    shippingData_id,
    invoice_address,
    shipping_country,
    giftRegistryData,
    RequestedByPaymentNotification,
    RequestedByUser,
    RequestedBySystem,
    CancellationDate,
    seller_name,
    seller_id,
    baseURL,
    name,
    lastChange,
    isCertified,
    lastMessage,
    authorizedDate,
    allowEdition,
    allowCancellation,
    shipping_complement,
    shipping_street,
    status,
    isCheckedIn,
    shipping_receiverName,
    subscriptionData,
    commercialConditionData,
    isCompleted,
    roundingError,
    changesAttachment_id,
    callCenterOperatorData,
    userPaymentInfo,
    orderGroup,
    creationDate,
    cancelReason,
    orderFormId,
    seller_logo,
    sellerOrderId,
    statusDescription,
    value,
    invoicedDate,
    customData,
    shipping_reference,
    merchantName,
    affiliateId,
    followUpEmail,
    userProfileId customer_id,
    hostname,
    checkedInPickupPointId,    
    origin,
    salesChannel,
    marketplaceServicesEndpoint,
    RequestedBySellerNotification,
    Reason,
    client_email email,
    taxData,
    shipping_number,
    openTextField,
    marketplaceOrderId,
    sequence,
    orderId
    FROM `shopstar-datalake.staging_zone.shopstar_ft_orders`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("ft_orders actualizado exitosamente")
  except:
    print("Error ft_orders!!")
    logging.exception("message")