import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join
import logging


def run():
  try:
    print("Creacion geoNetwork")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.geoNetwork` AS 
    SELECT 
    GENERATE_UUID() id_geoNetwork,
    visitId id_visitor,
    geoNetwork.continent continent,
    geoNetwork.subContinent subContinent,
    geoNetwork.country country,
    geoNetwork.region region,
    geoNetwork.metro metro,
    geoNetwork.city city,
    geoNetwork.cityId cityId,
    geoNetwork.networkDomain networkDomain, 
    geoNetwork.latitude latitude,
    geoNetwork.longitude longitude,
    geoNetwork.networkLocation networkLocation
    FROM `shopstar-datalake.191656782.ga_sessions*`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("geoNetwork actualizado exitosamente")
    create_visitors()
  except:
    print("Error geoNetwork!!")
    logging.exception("message")
    
def create_visitors():
  try:
    print("Creacion visitors")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.visitors` AS 
    SELECT GENERATE_UUID() id_visitor,
    visitorId,
    visitNumber,
    visitId,
    visitStartTime,
    date,
    fullVisitorId,
    userId,
    clientId,
    channelGrouping,
    socialEngagementType
    FROM `shopstar-datalake.191656782.ga_sessions*`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("visitors actualizado exitosamente")
    create_totals()
  except:
    print("Error visitors!!")
    logging.exception("message")
    
def create_totals():
  try:
    print("Creacion totals")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.totals` AS 
    SELECT GENERATE_UUID() id_Metric,
    visitId id_visitor,
    totals.visits visits,
    totals.hits hits,
    totals.pageviews pageviews,
    totals.timeOnSite timeOnSite,
    totals.bounces bounces,
    totals.transactions transactions,
    totals.transactionRevenue transactionRevenue,
    totals.newVisits newVisits,
    totals.screenviews screenviews,
    totals.uniqueScreenviews uniqueScreenviews,
    totals.timeOnScreen timeOnScreen,
    totals.totalTransactionRevenue totalTransactionRevenue,
    totals.sessionQualityDim 
    sessionQualityDim 
    FROM `shopstar-datalake.191656782.ga_sessions*`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("totals actualizado exitosamente")
    create_totals()
  except:
    print("Error totals!!")
    logging.exception("message")
    
def create_totals():
  try:
    print("Creacion totals")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.totals` AS 
    SELECT GENERATE_UUID() id_Metric,
    visitId id_visitor,
    totals.visits visits,
    totals.hits hits,
    totals.pageviews pageviews,
    totals.timeOnSite timeOnSite,
    totals.bounces bounces,
    totals.transactions transactions,
    totals.transactionRevenue transactionRevenue,
    totals.newVisits newVisits,
    totals.screenviews screenviews,
    totals.uniqueScreenviews uniqueScreenviews,
    totals.timeOnScreen timeOnScreen,
    totals.totalTransactionRevenue totalTransactionRevenue,
    totals.sessionQualityDim 
    sessionQualityDim 
    FROM `shopstar-datalake.191656782.ga_sessions*`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("totals actualizado exitosamente")
    create_trafficSource()
  except:
    print("Error totals!!")
    logging.exception("message")
    
def create_trafficSource():
  try:
    print("Creacion trafficSource")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.trafficSource` AS 
    SELECT GENERATE_UUID() id_trafficSource,
    visitId id_visitor,
    trafficSource.referralPath referralPath,
    trafficSource.campaign campaign,
    trafficSource.source source,
    trafficSource.medium medium,
    trafficSource.keyword keyword,
    trafficSource.adContent adContent,
    GENERATE_UUID() adwordsClickInfo,
    trafficSource.isTrueDirect isTrueDirect,
    trafficSource.campaignCode campaignCode 
    FROM `shopstar-datalake.191656782.ga_sessions*`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("trafficSource actualizado exitosamente")
    create_adwordsClickInfo()
  except:
    print("Error trafficSource!!")
    logging.exception("message")
    
def create_adwordsClickInfo():
  try:
    print("Creacion adwordsClickInfo")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.adwordsClickInfo` AS 
    SELECT GENERATE_UUID() id_adwordsClickInfo,
    trafficSource.adwordsClickInfo.adGroupId adGroupId,
    trafficSource.adwordsClickInfo.creativeId creativeId,
    trafficSource.adwordsClickInfo.criteriaId criteriaId,
    trafficSource.adwordsClickInfo.page page,
    trafficSource.adwordsClickInfo.slot slot,
    trafficSource.adwordsClickInfo.criteriaParameters criteriaParameters,
    trafficSource.adwordsClickInfo.gclId gclId,
    trafficSource.adwordsClickInfo.customerId customerId,
    trafficSource.adwordsClickInfo.adNetworkType adNetworkType,
    trafficSource.adwordsClickInfo.isVideoAd isVideoAd
    FROM `shopstar-datalake.191656782.ga_sessions*`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("adwordsClickInfo actualizado exitosamente")
  except:
    print("Error adwordsClickInfo!!")
    logging.exception("message")
    
def create_adwordsClickInfo():
  try:
    print("Creacion adwordsClickInfo")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.adwordsClickInfo` AS 
    SELECT GENERATE_UUID() id_adwordsClickInfo,
    trafficSource.adwordsClickInfo.adGroupId adGroupId,
    trafficSource.adwordsClickInfo.creativeId creativeId,
    trafficSource.adwordsClickInfo.criteriaId criteriaId,
    trafficSource.adwordsClickInfo.page page,
    trafficSource.adwordsClickInfo.slot slot,
    trafficSource.adwordsClickInfo.criteriaParameters criteriaParameters,
    trafficSource.adwordsClickInfo.gclId gclId,
    trafficSource.adwordsClickInfo.customerId customerId,
    trafficSource.adwordsClickInfo.adNetworkType adNetworkType,
    trafficSource.adwordsClickInfo.isVideoAd isVideoAd
    FROM `shopstar-datalake.191656782.ga_sessions*`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("adwordsClickInfo actualizado exitosamente")
    create_device()
  except:
    print("Error adwordsClickInfo!!")
    logging.exception("message")
    
def create_device():
  try:
    print("Creacion device")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.device` AS
    SELECT GENERATE_UUID() id_device,
    visitorId id_visitor,
    device.browser browser,
    device.browserVersion browserVersion,device.browserSize browserSize,
    device.operatingSystem operatingSystem,
    device.operatingSystemVersion operatingSystemVersion,
    device.isMobile isMobile,
    device.mobileDeviceBranding mobileDeviceBranding,
    device.mobileDeviceModel mobileDeviceModel,
    device.mobileInputSelector mobileInputSelector,
    device.mobileDeviceInfo mobileDeviceInfo,
    device.mobileDeviceMarketingName mobileDeviceMarketingName,
    device.flashVersion flashVersion,
    device.javaEnabled javaEnabled,
    device.language language,
    device.screenColors screenColors,
    device.screenResolution screenResolution,
    device.deviceCategory deviceCategory 
    FROM `shopstar-datalake.191656782.ga_sessions*` ''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("device actualizado exitosamente")
    create_hits()
  except:
    print("Error device!!")
    logging.exception("message")
    
def create_hits():
  try:
    print("Creacion hits")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.hits` AS 
    SELECT 
    GENERATE_UUID() id_hit,
    h.hitNumber hitNumber,
    h.time time,
    h.hour hour,
    h.minute minute,
    h.isSecure isSecure,
    h.isInteraction isInteraction,
    h.isEntrance isEntrance,
    h.isExit isExit,
    h.referer referer,
    h.transaction.transactionId id_transaction,
    h.type type,
    h.dataSource dataSource,
    h.uses_transient_token uses_transient_token
    FROM `shopstar-datalake.191656782.ga_sessions*`,UNNEST(hits) as h''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("hits actualizado exitosamente")
    create_transaction()
  except:
    print("Error hits!!")
    logging.exception("message")

def create_transaction():
  try:
    print("Creacion transaction")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.transaction` AS
    SELECT 
    GENERATE_UUID() id_transaction,
    h.transaction.transactionId transactionId,
    h.transaction.transactionRevenue transactionRevenue,
    h.transaction.transactionTax transactionTax,
    h.transaction.transactionShipping transactionShipping,
    h.transaction.affiliation affiliation,
    h.transaction.currencyCode currencyCode,
    h.transaction.localTransactionRevenue localTransactionRevenue,
    h.transaction.localTransactionTax localTransactionTax,
    h.transaction.localTransactionShipping localTransactionShipping,
    h.transaction.transactionCoupon transactionCoupon
    FROM `shopstar-datalake.191656782.ga_sessions*`,UNNEST(hits) as h''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    create_product()
    print("transaction actualizado exitosamente")
  except:
    print("Error transaction!!")
    logging.exception("message")
    
def create_product():
  try:
    print("Creacion transaction")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.product` AS 
    SELECT
    GENERATE_UUID() id_product,
    visitorId id_visitor,
    product.productSKU productSKU,
    product.v2ProductName v2ProductName,
    product.v2ProductCategory v2ProductCategory,
    product.productVariant productVariant,
    product.productBrand productBrand,
    product.productRevenue productRevenue,
    product.localProductRevenue localProductRevenue,
    product.productPrice productPrice,
    product.localProductPrice localProductPrice,
    product.productQuantity productQuantity,
    product.productRefundAmount productRefundAmount,
    product.localProductRefundAmount localProductRefundAmount,
    product.isImpression isImpression,
    product.isClick isClick
    FROM
    `shopstar-datalake.191656782.ga_sessions*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("transaction actualizado exitosamente")
  except:
    print("Error transaction!!")
    logging.exception("message")
run()