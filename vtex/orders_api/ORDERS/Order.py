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

class init:
    productList = []
    df = pd.DataFrame()
    cache = 0
    emailTracked = "null"
    approvedBy = "null"
    cancelledBy = "null"
    cancelReason = "null"
    orderId = "null"
    sequence = "null"
    marketplaceOrderId = "null"
    marketplaceServicesEndpoint = "null"
    sellerOrderId = "null"
    origin = "null"
    affiliateId = "null"
    salesChannel = "null"
    merchantName = "null"
    status = "null"
    statusDescription = "null"
    value = "null"
    creationDate = "null"
    lastChange = "null"
    orderGroup = "null"
    giftRegistryData = "null"
    marketingData = "null"
    callCenterOperatorData = "null"
    followUpEmail = "null"
    lastMessage = "null"
    hostname = "null"
    invoiceData = "null"
    openTextField = "null"
    roundingError = "null"
    orderFormId = "null"
    commercialConditionData = "null"
    isCompleted = "null"
    customData = "null"
    allowCancellation = "null"
    allowEdition = "null"
    isCheckedIn = "null"
    authorizedDate = "null"
    invoicedDate = "null"
    
    '''
    Dimensiones TOTAL
    '''
    
    total_id_items = "null"
    total_name_items = "null"
    total_value_items = "null"
    total_id_discounts = "null"
    total_name_discounts = "null"
    total_value_discounts = "null"
    total_id_shipping = "null"
    total_name_shipping = "null"
    total_value_shipping = "null"
    total_id_tax = "null"
    total_name_tax = "null"
    total_value_tax = "null"
    total_id_change = "null"
    total_name_change = "null"
    total_value_change = "null"

    '''
    Dimensiones ITEMS
    '''

    items_uniqueId = "null"
    items_id = "null"
    items_productId = "null"
    items_ean = "null"
    items_lockId = "null"
    item_quantity = "null"
    item_seller = "null"
    item_name = "null"
    item_refId = "null"
    item_price = "null"
    item_listPrice = "null"
    item_manualPrice = "null"
    item_imageUrl = "null"
    item_detailUrl = "null"
    item_sellerSku = "null"
    item_priceValidUntil = "null"
    item_commission = "null"
    item_tax = "null"
    item_preSaleDate = "null"
    item_itemAttachment_name = "null"
    item_measurementUnit = "null"
    item_unitMultiplier = "null"
    item_sellingPrice = "null"
    item_isGift = "null"
    item_shippingPrice = "null"
    item_rewardValue = "null"
    item_freightCommission = "null"
    item_taxCode = "null"
    item_parentItemIndex = "null"
    item_parentAssemblyBinding = "null"
    item_price_definition = "null"
    item_serialNumbers = "null"
    
    '''
    Dimensiones ITEMS_INFORMATION__ADITIONAL
    '''
    brandName = "null"
    brandId = "null"
    categoriesIds = "null"
    productClusterId = "null"
    commercialConditionId = "null"
    offeringInfo = "null"
    offeringType = "null"
    offeringTypeId = "null"
    '''
    Dimensiones ITEMS_INFORMATION__ADITIONAL_dimension
    '''
    cubicweight = "null"
    height = "null"
    length = "null"
    weight = "null"
    width = "null"
    '''
    Dimensiones ITEMS_priceDefinition
    '''
    total = "null"
    calculatedSellingPrice = "null"
    
    '''
    clientProfileData
    '''
    
    client_id = "null"
    client_email = "null"
    client_firstName = "null"
    client_lastName = "null"
    client_documentType = "null"
    client_document = "null"
    client_phone = "null"
    client_corporateName = "null"
    client_tradeName = "null"
    client_corporateDocument = "null"
    client_stateInscription = "null"
    client_corporatePhone = "null"
    client_isCorporate = "null"
    client_userProfileId = "null"
    client_customerClass = "null"
    
    '''
    "ratesAndBenefitsData"
    '''
    
    id_ratesAndBenefits = "null"
    
    '''
    "shippingData"
    '''
    
    shippingData_id = "null"
    
    '''
    address
    '''
    
    addressType = "null"
    receiverName = "null"
    addressId = "null"
    postalCode = "null"
    city = "null"
    state = "null"
    country = "null"
    street = "null"
    number = "null"
    neighborhood = "null"
    complement = "null"
    reference = "null"
    
    '''
    logisticsInfo
    '''
    deliveryChannel = "null"
    trackingHints = "null"
    addressId = "null"
    polygonName = "null"
    itemIndex = "null"
    selectedSla = "null"
    lockTTL = "null"
    price = "null"
    listPrice = "null"
    sellingPrice = "null"
    deliveryWindow = "null"
    deliveryCompany = "null"
    shippingEstimate = "null"
    shippingEstimateDate = "null"
    
    '''
    slas
    '''
    slas_id = "null"
    slas_name = "null"
    slas_shippingEstimate = "null"
    slas_deliveryWindow = "null"
    slas_price = "null"
    slas_deliveryChannel = "null"
    slas_polygonName = "null"
    '''
    slas_pickupStoreInfo
    '''
    slas_pickupStoreInfo_additionalInfo = "null"
    slas_pickupStoreInfo_address = "null"
    slas_pickupStoreInfo_dockId = "null"
    slas_pickupStoreInfo_friendlyName = "null"
    slas_pickupStoreInfo_isPickupStore = "null"
    
    '''
    slas_1
    '''
    slas_id_01 = "null"
    slas_name_01 = "null"
    slas_shippingEstimate_01 = "null"
    slas_deliveryWindow_01 = "null"
    slas_price_01 = "null"
    slas_deliveryChannel_01 = "null"
    slas_polygonName_01 = "null"
    '''
    slas_pickupStoreInfo_1
    '''
    slas_pickupStoreInfo_additionalInfo_01 = "null"
    slas_pickupStoreInfo_address_01 = "null"
    slas_pickupStoreInfo_dockId_01 = "null"
    slas_pickupStoreInfo_friendlyName_01 = "null"
    slas_pickupStoreInfo_isPickupStore_01 = "null"
    
    '''
    slas_2
    '''
    slas_id_02 = "null"
    slas_name_02 = "null"
    slas_shippingEstimate_02 = "null"
    slas_deliveryWindow_02 = "null"
    slas_price_02 = "null"
    slas_deliveryChannel_02 = "null"
    slas_polygonName_02 = "null"
    '''
    slas_pickupStoreInfo_2
    '''
    slas_pickupStoreInfo_additionalInfo_02 = "null"
    slas_pickupStoreInfo_address_02 = "null"
    slas_pickupStoreInfo_dockId_02 = "null"
    slas_pickupStoreInfo_friendlyName_02 = "null"
    slas_pickupStoreInfo_isPickupStore_02 = "null"
    
    '''
    slas_3
    '''
    slas_id_03 = "null"
    slas_name_03 = "null"
    slas_shippingEstimate_03 = "null"
    slas_deliveryWindow_03 = "null"
    slas_price_03 = "null"
    slas_deliveryChannel_03 = "null"
    slas_polygonName_03 = "null"
    '''
    slas_pickupStoreInfo_3
    '''
    slas_pickupStoreInfo_additionalInfo_03 = "null"
    slas_pickupStoreInfo_address_03 = "null"
    slas_pickupStoreInfo_dockId_03 = "null"
    slas_pickupStoreInfo_friendlyName_03 = "null"
    slas_pickupStoreInfo_isPickupStore_03 = "null"
    
    '''
    deliveryIds
    '''
    courierId = "null"
    courierName = "null"
    dockId = "null"
    quantity = "null"
    warehouseId = "null"
    
    '''
    pickupStoreInfo
    '''
    pickupStoreInfo_additionalInfo = "null"
    pickupStoreInfo_address = "null"
    pickupStoreInfo_dockId = "null"
    pickupStoreInfo_friendlyName = "null"
    pickupStoreInfo_isPickupStore = "null"
    
    '''
    selectedAddresses
    '''
    
    selectedAddresses_addressId = "null"
    selectedAddresses_addressType = "null"
    selectedAddresses_receiverName = "null"
    selectedAddresses_street = "null"
    selectedAddresses_number = "null"
    selectedAddresses_complement = "null"
    selectedAddresses_neighborhood = "null"
    selectedAddresses_postalCode = "null"
    selectedAddresses_city = "null"
    selectedAddresses_state = "null"
    selectedAddresses_country = "null"
    selectedAddresses_reference = "null"
    
    '''
    transactions
    '''
    transactions_isActive = "null"
    transactions_transactionId = "null"
    transactions_merchantName = "null"
    
    '''
    payments
    '''
    
    payments_id = "null"
    payments_paymentSystem = "null"
    payments_paymentSystemName = "null"
    payments_value = "null"
    payments_installments = "null"
    payments_referenceValue = "null"
    payments_cardHolder = "null"
    payments_firstDigits = "null"
    payments_lastDigits = "null"
    payments_url = "null"
    payments_giftCardId = "null"
    payments_giftCardName = "null"
    payments_giftCardCaption = "null"
    payments_redemptionCode = "null"
    payments_group = "null"
    payments_tid = "null"
    payments_dueDate = "null"
    payments_cardNumber = "null"
    payments_cvv2 = "null"
    payments_expireMonth = "null"
    payments_expireYear = "null"
    payments_giftCardProvider = "null"
    payments_giftCardAsDiscount = "null"
    payments_koinUrl = "null"
    payments_accountId = "null"
    payments_parentAccountId = "null"
    payments_bankIssuedInvoiceIdentificationNumber = "null"
    payments_bankIssuedInvoiceIdentificationNumberFormatted = "null"
    payments_ankIssuedInvoiceBarCodeNumber = "null"
    payments_bankIssuedInvoiceBarCodeType = "null"
    
    payments_Tid = "null"
    payments_ReturnCode = "null"
    payments_Message = "null"
    payments_authId = "null"
    payments_acquirer = "null"

    '''
    billingAddress
    '''
    
    billingAddress_postalCode = "null"
    billingAddress_city = "null"
    billingAddress_state = "null"
    billingAddress_country = "null"
    billingAddress_street = "null"
    billingAddress_number = "null"
    billingAddress_neighborhood = "null"
    billingAddress_complement = "null"
    billingAddress_reference = "null"
    
    '''
    Sellers
    '''
    seller_id = "null"
    seller_name = "null"
    seller_logo = "null"
    
    '''
    changesAttachment
    '''
    changesAttachment_id = "null"
    
    '''
    storePreferencesData
    '''
    storePreferencesData_countryCode = "null"
    storePreferencesData_currencyCode = "null"
    storePreferencesData_currencyLocale = "null"
    storePreferencesData_currencySymbol = "null"
    storePreferencesData_timeZone = "null"
    
    '''
    currencyFormatInfo
    '''
    
    CurrencyDecimalDigits = "null"
    CurrencyDecimalSeparator = "null"
    CurrencyGroupSeparator = "null"
    CurrencyGroupSize = "null"
    StartsWithCurrencySymbol = "null"
    
    '''
    currencyFormatInfo
    '''
    
    baseURL = "null"
    isCertified = "null"
    name = "null"
    
    '''
    itemMetadata
    '''
    itemMetadata_Id = "null"
    itemMetadata_Seller = "null"
    itemMetadata_Name = "null"
    itemMetadata_SkuName = "null"
    itemMetadata_ProductId = "null"
    itemMetadata_RefId = "null"
    itemMetadata_Ean = "null"
    itemMetadata_ImageUrl = "null"
    itemMetadata_DetailUrl = "null"
    
    subscriptionData = "null"
    taxData = "null"
    checkedInPickupPointId = "null"
    cancellationData = "null"
    
    '''
    packageAttachment
    '''
    courier = "null"
    invoiceNumber = "null"
    invoiceValue = "null"
    invoiceUrl = "null"
    issuanceDate = "null"
    trackingNumber = "null"
    invoiceKey = "null"
    trackingUrl = "null"
    embeddedInvoice = "null"
    type = "null"
    courierStatus = "null"
    cfop = "null"
    restitutions = "null"
    volumes = "null"
    EnableInferItems = "null"
    
    '''
    invoice data
    '''
    invoice_address = "null"
    userPaymentInfo = "null"
    
    '''
    transacctions
    '''
    
    isActive = "null"
    transactionId = "null"
    merchantName = "null"
    
    '''
    cancellation
    '''
    RequestedByUser = "null"
    RequestedBySystem = "null"
    RequestedBySellerNotification = "null"
    RequestedByPaymentNotification = "null"
    Reason = "null"
    CancellationDate = "null"
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def dicMemberCheck(key, dicObj):
    if key in dicObj:
        return True
    else:
        return False

def decrypt_email(email):
    try:
        url = "https://conversationtracker.vtex.com.br/api/pvt/emailMapping?an=mercury&alias="+email+""
        headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
        response = requests.request("GET", url, headers=headers)
        formatoJ = json.loads(response.text)
        return formatoJ["email"]
    except:
        init.cache = 2
     
def get_order(id,reg):
    try:
        print("Registro: "+str(reg))
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        
        if "subscriptionData" in Fjson:
            init.subscriptionData = Fjson["subscriptionData"]
        if "taxData" in Fjson:
            init.taxData = Fjson["taxData"]
        if "checkedInPickupPointId" in Fjson:
            init.checkedInPickupPointId = Fjson["checkedInPickupPointId"]
        if "cancellationData" in Fjson:
            init.cancellationData = Fjson["cancellationData"]
        
        if "emailTracked" in Fjson:
        	init.emailTracked = Fjson["emailTracked"]
        if "approvedBy" in Fjson:
        	init.approvedBy = Fjson["approvedBy"]
        if "cancelledBy" in Fjson:
        	init.cancelledBy = Fjson["cancelledBy"]
        if "cancelReason" in Fjson:
        	init.cancelReason = Fjson["cancelReason"]
        if "orderId" in Fjson:
        	init.orderId = Fjson["orderId"]
        if "sequence" in Fjson:
        	init.sequence = Fjson["sequence"]
        if "marketplaceOrderId" in Fjson:
        	init.marketplaceOrderId = Fjson["marketplaceOrderId"]
        if "marketplaceServicesEndpo" in Fjson:
        	init.marketplaceServicesEndpoint = Fjson["marketplaceServicesEndpoint"]
        if "sellerOrderId" in Fjson:
        	init.sellerOrderId = Fjson["sellerOrderId"]
        if "origin" in Fjson:
        	init.origin = Fjson["origin"]
        if "affiliateId" in Fjson:
        	init.affiliateId = Fjson["affiliateId"]
        if "salesChannel" in Fjson:
        	init.salesChannel = Fjson["salesChannel"]
        if "merchantName" in Fjson:
        	init.merchantName = Fjson["merchantName"]
        if "status" in Fjson:
        	init.status = Fjson["status"]
        if "statusDescription" in Fjson:
        	init.statusDescription = Fjson["statusDescription"]
        if "value" in Fjson:
        	init.value = Fjson["value"]
        if "creationDate" in Fjson:
        	init.creationDate = Fjson["creationDate"]
        if "lastChange" in Fjson:
        	init.lastChange = Fjson["lastChange"]
        if "orderGroup" in Fjson:
        	init.orderGroup = Fjson["orderGroup"]
        if "giftRegistryData" in Fjson:
        	init.giftRegistryData = Fjson["giftRegistryData"]
        if "marketingData" in Fjson:
        	init.marketingData = Fjson["marketingData"]
        if "callCenterOperatorData" in Fjson:
        	init.callCenterOperatorData = Fjson["callCenterOperatorData"]
        if "followUpEmail" in Fjson:
        	init.followUpEmail = Fjson["followUpEmail"]
        if "lastMessage" in Fjson:
        	init.lastMessage = Fjson["lastMessage"]
        if "hostname" in Fjson:
        	init.hostname = Fjson["hostname"]
        if "invoiceData" in Fjson:
        	init.invoiceData = Fjson["invoiceData"]
        if "openTextField" in Fjson:
        	init.openTextField = Fjson["openTextField"]
        if "roundingError" in Fjson:
        	init.roundingError = Fjson["roundingError"]
        if "orderFormId" in Fjson:
        	init.orderFormId = Fjson["orderFormId"]
        if "commercialConditionData" in Fjson:
        	init.commercialConditionData = Fjson["commercialConditionData"]
        if "isCompleted" in Fjson:
        	init.isCompleted = Fjson["isCompleted"]
        if "customData" in Fjson:
        	init.customData = Fjson["customData"]
        if "allowCancellation" in Fjson:
        	init.allowCancellation = Fjson["allowCancellation"]
        if "allowEdition" in Fjson:
        	init.allowEdition = Fjson["allowEdition"]
        if "isCheckedIn" in Fjson:
        	init.isCheckedIn = Fjson["isCheckedIn"]
        if "authorizedDate" in Fjson:
        	init.authorizedDate = Fjson["authorizedDate"]
        if "invoicedDate" in Fjson:
        	init.invoicedDate = Fjson["invoicedDate"]
        
        
        '''
        INIT DIMENSION  packageAttachment
        '''    
        try:
            packageAttachment = Fjson["packageAttachment"]
            packages = packageAttachment["packages"]
        except:
            init.cache = 2 
        try:
            itemMetadata = Fjson["itemMetadata"]
            ItemMetadata = itemMetadata["Items"]
        except:
            init.cache = 2
        try:
            Total = Fjson["totals"]
        except:
            init.cache = 2
        try:
            clientProfileData = Fjson["clientProfileData"]
        except:
            init.cache = 2
        try:
            marketplace = Fjson["marketplace"]
        except:
           init.cache = 2
        try:
            ratesAndBenefitsData = Fjson["ratesAndBenefitsData"]
        except:
            init.cache = 2
        try:
            storePreferencesData = Fjson["storePreferencesData"]
        except:
            init.cache = 2
        try:
            currencyFormatInfo = storePreferencesData["currencyFormatInfo"]
        except:
            init.cache = 2
        try:
            shippingData = Fjson["shippingData"]
        except:
            init.cache = 2
        try:
            logisticsInfo_0 = shippingData["logisticsInfo"]
        except:
            init.cache = 2
        try:
            selectedAddresses_ = shippingData["selectedAddresses"]
        except:
            init.cache = 2
        try:
            selectedAddresses = selectedAddresses_[0]
        except:
            init.cache = 2
        try:
            logisticsInfo = logisticsInfo_0[0]
        except:
            init.cache = 2
        try:
            address = shippingData["address"]
        except:
            init.cache = 2
        try:
            slas = logisticsInfo["slas"]
        except:
            init.cache = 2
        try:
            deliveryIds_ = logisticsInfo["deliveryIds"]
        except:
            init.cache = 2
        try:
            deliveryIds = deliveryIds_[0]
        except:
            init.cache = 2
        try:
            pickupStoreInfo = logisticsInfo["pickupStoreInfo"]
        except:
            init.cache = 2
        try:
            slas_0 = slas[0]
            pickupStoreInfo = slas_0["pickupStoreInfo"]
        except:
            init.cache = 2
        try:
            slas_1 = slas[1]
            pickupStoreInfo_1 = slas_1["pickupStoreInfo"]
        except:
            init.cache = 2
        try:
            slas_2 = slas[2]
            pickupStoreInfo_2 = slas_2["pickupStoreInfo"]
        except:
            init.cache = 2
        try:
            slas_3 = slas[3]
            pickupStoreInfo_3 = slas_3["pickupStoreInfo"]
        except:
            init.cache = 2
        try:
            items = Fjson["items"]
        except:
            init.cache = 2
        try:
            changesAttachment = Fjson["changesAttachment"]
        except:
            init.cache = 2
        try:
            paymentData = Fjson["paymentData"]
            transactions = paymentData["transactions"]
        except:
            init.cache = 2
        try:
            sellers_ = Fjson["sellers"]
        except:
            init.cache = 2
        try:
            sellers = sellers_[0]
        except:
            init.cache = 2
        try:
            transactions_ = paymentData["transactions"]
        except:
            init.cache = 2
        try:
            transactions = transactions_[0]
        except:
            init.cache = 2
        try:
            payments_ = transactions["payments"]
        except:
            init.cache = 2
        try:
            payments = payments_[0]
        except:
            init.cache = 2
        try:
            billingAddress = payments["billingAddress"]
        except:
            init.cache = 2
        try:
            Items = items[0]
        except:
            init.cache = 2
        try:
            itemAttachment = Items["itemAttachment"]
        except:
            init.cache = 2
        try:
            additionalInfo = Items["additionalInfo"]
        except:
            init.cache = 2
        try:
            priceDefinition = Items["priceDefinition"]
        except:
            init.cache = 2
        try:
            sellingPrice = Items["sellingPrice"]
        except:
            init.cache = 2
        try:
            dimension = additionalInfo["dimension"]
        except:
            init.cache = 2
        
        '''
        END DIMENSION
        '''
        
        
        try:
            if Total[0]:
                items = Total[0]
                init.total_id_items = items["id"]
                init.total_name_items = items["name"]
                init.total_value_items = items["value"]
        except:
            init.cache = 2
        try:
            if Total[1]:
                discounts = Total[1]
                init.total_id_discounts = discounts["id"]
                init.total_name_discounts = discounts["name"]
                init.total_value_discounts = discounts["value"]
        except:
            init.cache = 2
        try:
            if Total[2]:
                shipping = Total[2]
                init.total_id_shipping = shipping["id"]
                init.total_name_shipping = shipping["name"]
                init.total_value_shipping = shipping["value"]
        except:
            init.cache = 2
        try:
            if Total[3]:
                tax = Total[3]
                init.total_id_tax = tax["id"]
                init.total_name_tax = tax["name"]
                init.total_value_tax = tax["value"]
        except:
            init.cache = 2
        try:
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
        except:
            init.cache = 2
        try:
            init.brandName = additionalInfo["brandName"]
            init.brandId = additionalInfo["brandId"]
            init.categoriesIds = additionalInfo["categoriesIds"]
            init.productClusterId = additionalInfo["productClusterId"]
            init.commercialConditionId = additionalInfo["commercialConditionId"]
            init.offeringInfo = additionalInfo["offeringInfo"]
            init.offeringType = additionalInfo["offeringType"]
            init.offeringTypeId = additionalInfo["offeringTypeId"]
        except:
            init.cache = 2
        try:
            init.cubicweight = dimension["cubicweight"]
            init.height = dimension["height"]
            init.length = dimension["length"]
            init.weight = dimension["weight"]
            init.width = dimension["width"]
        except:
            init.cache = 2
        try:
            init.item_itemAttachment_name = itemAttachment["name"]
        except:
            init.cache = 2
        try:
            init.client_id = clientProfileData["id"]
            init.client_email = clientProfileData["email"]
            init.client_firstName = clientProfileData["firstName"]
            init.client_lastName = clientProfileData["lastName"]
            init.client_documentType = clientProfileData["documentType"]
            init.client_document = clientProfileData["document"]
            init.client_phone = clientProfileData["phone"]
            init.client_corporateName = clientProfileData["corporateName"]
            init.client_tradeName = clientProfileData["tradeName"]
            init.client_corporateDocument = clientProfileData["corporateDocument"]
            init.client_stateInscription = clientProfileData["stateInscription"]
            init.client_corporatePhone = clientProfileData["corporatePhone"]
            init.client_isCorporate = clientProfileData["isCorporate"]
            init.client_userProfileId = clientProfileData["userProfileId"]
            init.client_customerClass = clientProfileData["customerClass"]     
        except:
            init.cache = 2
        try:
            init.id_ratesAndBenefits = ratesAndBenefitsData["id"]
        except:
            init.cache = 2
        try:
            init.shippingData_id = shippingData["id"]
        except:
            init.cache = 2
        try:
            init.addressType = address["addressType"]
            init.receiverName = address["receiverName"]
            init.addressId = address["addressId"]
            init.postalCode = address["postalCode"]
            init.city = address["city"]
            init.state = address["state"]
            init.country = address["country"]
            init.street = address["street"]
            init.number = address["number"]
            init.neighborhood = address["neighborhood"]
            init.complement = address["complement"]
            init.reference = address["reference"]
        except:
            init.cache = 2
            
        try:
            init.trackingHints = logisticsInfo_0[0]
            init.deliveryChannel = logisticsInfo["deliveryChannel"]
            init.addressId = logisticsInfo["addressId"]
            init.polygonName = logisticsInfo["polygonName"]
            init.itemIndex = logisticsInfo["itemIndex"]
            init.selectedSla = logisticsInfo["selectedSla"]
            init.lockTTL = logisticsInfo["lockTTL"]
            init.price = logisticsInfo["price"]
            init.listPrice = logisticsInfo["listPrice"]
            init.sellingPrice = logisticsInfo["sellingPrice"]
            init.deliveryWindow = logisticsInfo["deliveryWindow"]
            init.deliveryCompany = logisticsInfo["deliveryCompany"]
            init.shippingEstimate = logisticsInfo["shippingEstimate"]
            init.shippingEstimateDate = logisticsInfo["shippingEstimateDate"]
        except:
            init.cache = 2
        try:
            init.slas_id = slas_0["id"]
            init.slas_name = slas_0["name"]
            init.slas_shippingEstimate = slas_0["shippingEstimate"]
            init.slas_deliveryWindow = slas_0["deliveryWindow"]
            init.slas_price = slas_0["price"]
            init.slas_deliveryChannel = slas_0["deliveryChannel"]
            init.slas_polygonName = slas_0["polygonName"]
        except:
            init.cache = 2
            
        try:
            init.slas_pickupStoreInfo_additionalInfo = pickupStoreInfo["additionalInfo"]
            init.slas_pickupStoreInfo_address = pickupStoreInfo["address"]
            init.slas_pickupStoreInfo_dockId = pickupStoreInfo["dockId"]
            init.slas_pickupStoreInfo_friendlyName = pickupStoreInfo["friendlyName"]
            init.slas_pickupStoreInfo_isPickupStore = pickupStoreInfo["isPickupStore"]
        except:
            init.cache = 2
        try:
            init.slas_id_01 = slas_1["id"]
            init.slas_name_01 = slas_1["name"]
            init.slas_shippingEstimate_01 = slas_1["shippingEstimate"]
            init.slas_deliveryWindow_01 = slas_1["deliveryWindow"]
            init.slas_price_01 = slas_1["price"]
            init.slas_deliveryChannel_01 = slas_1["deliveryChannel"]
            init.slas_polygonName_01 = slas_1["polygonName"]
        except:
            init.cache = 2
        try:
            init.slas_pickupStoreInfo_additionalInfo_01 = pickupStoreInfo_1["additionalInfo"]
            init.slas_pickupStoreInfo_address_01 = pickupStoreInfo_1["address"]
            init.slas_pickupStoreInfo_dockId_01 = pickupStoreInfo_1["dockId"]
            init.slas_pickupStoreInfo_friendlyName_01 = pickupStoreInfo_1["friendlyName"]
            init.slas_pickupStoreInfo_isPickupStore_01 = pickupStoreInfo_1["isPickupStore"]
        except:
            init.cache = 2
        
        try:
            init.slas_id_02 = slas_2["id"]
            init.slas_name_02 = slas_2["name"]
            init.slas_shippingEstimate_02 = slas_2["shippingEstimate"]
            init.slas_deliveryWindow_02 = slas_2["deliveryWindow"]
            init.slas_price_02 = slas_2["price"]
            init.slas_deliveryChannel_02 = slas_2["deliveryChannel"]
            init.slas_polygonName_02 = slas_2["polygonName"]
        except:
            init.cache = 2
        try:
            init.slas_pickupStoreInfo_additionalInfo_02 = pickupStoreInfo_2["additionalInfo"]
            init.slas_pickupStoreInfo_address_02 = pickupStoreInfo_2["address"]
            init.slas_pickupStoreInfo_dockId_02 = pickupStoreInfo_2["dockId"]
            init.slas_pickupStoreInfo_friendlyName_02 = pickupStoreInfo_2["friendlyName"]
            init.slas_pickupStoreInfo_isPickupStore_02 = pickupStoreInfo_2["isPickupStore"]
        except:
            init.cache = 2
            
        try:
            init.slas_id_03 = slas_3["id"]
            init.slas_name_03 = slas_3["name"]
            init.slas_shippingEstimate_03 = slas_3["shippingEstimate"]
            init.slas_deliveryWindow_03 = slas_3["deliveryWindow"]
            init.slas_price_03 = slas_3["price"]
            init.slas_deliveryChannel_03 = slas_3["deliveryChannel"]
            init.slas_polygonName_03 = slas_3["polygonName"]
        except:
            init.cache = 2
        try: 
            init.slas_pickupStoreInfo_additionalInfo_03 = pickupStoreInfo_3["additionalInfo"]
            init.slas_pickupStoreInfo_address_03 = pickupStoreInfo_3["address"]
            init.slas_pickupStoreInfo_dockId_03 = pickupStoreInfo_3["dockId"]
            init.slas_pickupStoreInfo_friendlyName_03 = pickupStoreInfo_3["friendlyName"]
            init.slas_pickupStoreInfo_isPickupStore_03 = pickupStoreInfo_3["isPickupStore"]
        except:
            init.cache = 2
        try:
            init.courierId = deliveryIds["courierId"]
            init.courierName = deliveryIds["courierName"]
            init.dockId = deliveryIds["dockId"]
            init.quantity = deliveryIds["quantity"]
            init.warehouseId = deliveryIds["warehouseId"]
        except:
            init.cache = 2
        try: 
            init.pickupStoreInfo_additionalInfo = pickupStoreInfo["additionalInfo"]
            init.pickupStoreInfo_address = pickupStoreInfo["address"]
            init.pickupStoreInfo_dockId = pickupStoreInfo["dockId"]
            init.pickupStoreInfo_friendlyName = pickupStoreInfo["friendlyName"]
            init.pickupStoreInfo_isPickupStore = pickupStoreInfo["isPickupStore"]
        except:
            init.cache = 2
        try:
            init.selectedAddresses_addressId = selectedAddresses["addressId"]
            init.selectedAddresses_addressType = selectedAddresses["addressType"]
            init.selectedAddresses_receiverName = selectedAddresses["receiverName"]
            init.selectedAddresses_street = selectedAddresses["street"]
            init.selectedAddresses_number = selectedAddresses["number"]
            init.selectedAddresses_complement = selectedAddresses["complement"]
            init.selectedAddresses_neighborhood = selectedAddresses["neighborhood"]
            init.selectedAddresses_postalCode = selectedAddresses["postalCode"]
            init.selectedAddresses_city = selectedAddresses["city"]
            init.selectedAddresses_state = selectedAddresses["state"]
            init.selectedAddresses_country = selectedAddresses["country"]
            init.selectedAddresses_reference = selectedAddresses["reference"]
        except:
            init.cache = 2
        try:
            init.transactions_isActive = transactions["isActive"]
            init.transactions_transactionId = transactions["transactionId"]
            init.transactions_merchantName = transactions["merchantName"]
        except:
            init.cache = 2
        try:
            init.payments_id = payments["id"]
            init.payments_paymentSystem = payments["paymentSystem"]
            init.payments_paymentSystemName = payments["paymentSystemName"]
            init.payments_value = payments["value"]
            init.payments_installments = payments["installments"]
            init.payments_referenceValue = payments["referenceValue"]
            init.payments_cardHolder = payments["cardHolder"]
            init.payments_firstDigits = payments["firstDigits"]
            init.payments_lastDigits = payments["lastDigits"]
            init.payments_url = payments["url"]
            init.payments_giftCardId = payments["giftCardId"]
            init.payments_giftCardName = payments["giftCardName"]
            init.payments_giftCardCaption = payments["giftCardCaption"]
            init.payments_redemptionCode = payments["redemptionCode"]
            init.payments_group = payments["group"]
            init.payments_tid = payments["tid"]
            init.payments_dueDate = payments["dueDate"]
            init.payments_cardNumber = payments["cardNumber"]
            init.payments_cvv2 = payments["cvv2"]
            init.payments_expireMonth = payments["expireMonth"]
            init.payments_expireYear = payments["expireYear"]
            init.payments_giftCardProvider = payments["giftCardProvider"]
            init.payments_giftCardAsDiscount = payments["giftCardAsDiscount"]
            init.payments_koinUrl = payments["koinUrl"]
            init.payments_accountId = payments["accountId"]
            init.payments_parentAccountId = payments["parentAccountId"]
            init.payments_bankIssuedInvoiceIdentificationNumber = payments["bankIssuedInvoiceIdentificationNumber"]
            init.payments_bankIssuedInvoiceIdentificationNumberFormatted = payments["bankIssuedInvoiceIdentificationNumberFormatted"]
            init.payments_bankIssuedInvoiceBarCodeNumber = payments["bankIssuedInvoiceBarCodeNumber"]
            init.payments_bankIssuedInvoiceBarCodeType = payments["bankIssuedInvoiceBarCodeType"]
        except:
            init.cache = 2
        try:
            connectorResponses = payments["connectorResponses"]
            init.payments_ReturnCode = connectorResponses["ReturnCode"]
            init.payments_Message = connectorResponses["Message"]
            init.payments_authId = connectorResponses["authId"]
            init.payments_acquirer = connectorResponses["acquirer"]
        except:
            init.cache = 2
        try:
            init.billingAddress_postalCode = billingAddress["postalCode"]
            init.billingAddress_city = billingAddress["city"]
            init.billingAddress_state = billingAddress["state"]
            init.billingAddress_country = billingAddress["country"]
            init.billingAddress_street = billingAddress["street"]
            init.billingAddress_number = billingAddress["number"]
            init.billingAddress_neighborhood = billingAddress["neighborhood"]
            init.billingAddress_complement = billingAddress["complement"]
            init.billingAddress_reference = billingAddress["reference"]
        except:
            init.cache = 2
        try:
            init.seller_id = sellers["id"]
            init.seller_name = sellers["name"]
            init.seller_logo = sellers["logo"]
        except:
            init.cache = 2
        try:
            init.changesAttachment_id = Fjson["changesAttachment"]
        except:
            init.cache = 2
        try:
            init.storePreferencesData_countryCode = storePreferencesData["countryCode"]
            init.storePreferencesData_currencyCode = storePreferencesData["currencyCode"]
            init.storePreferencesData_currencyLocale = storePreferencesData["currencyLocale"]
            init.storePreferencesData_currencySymbol = storePreferencesData["currencySymbol"]
            init.storePreferencesData_timeZone = storePreferencesData["timeZone"]
        except:
            init.cache = 2
        try:
            init.CurrencyDecimalDigits = currencyFormatInfo["CurrencyDecimalDigits"]
            init.CurrencyDecimalSeparator = currencyFormatInfo["CurrencyDecimalSeparator"]
            init.CurrencyGroupSeparator = currencyFormatInfo["CurrencyGroupSeparator"]
            init.CurrencyGroupSize = currencyFormatInfo["CurrencyGroupSize"]
            init.StartsWithCurrencySymbol = currencyFormatInfo["StartsWithCurrencySymbol"]
        except:
            init.cache = 2
        try:
            init.baseURL = marketplace["baseURL"]
            init.isCertified = marketplace["isCertified"]
            init.name = marketplace["name"]
        except:
            init.cache = 2
        
        try:
            client_email = decrypt_email(str(init.client_email))
        except:
            client_email = None
            
        
        
        
        
        try:
            for x in ItemMetadata:
                init.itemMetadata_Id = x["Id"]
                init.itemMetadata_Seller = x["Seller"]
                init.itemMetadata_Name = x["Name"]
                init.itemMetadata_SkuName = x["SkuName"]
                init.itemMetadata_ProductId = x["ProductId"]
                init.itemMetadata_RefId = x["RefId"]
                init.itemMetadata_Ean = x["Ean"]
                init.itemMetadata_ImageUrl = x["ImageUrl"]
                init.itemMetadata_DetailUrl = x["DetailUrl"]
        except:
            init.cache = 2
        
        try:
            for x in packages:
                init.courier = x["courier"]
                init.invoiceNumber = x["invoiceNumber"]
                init.invoiceValue = x["invoiceValue"]
                init.invoiceUrl = x["invoiceUrl"]
                init.issuanceDate = x["issuanceDate"]
                init.trackingNumber = x["trackingNumber"]
                init.invoiceKey = x["invoiceKey"]
                init.trackingUrl = x["trackingUrl"]
                init.embeddedInvoice = x["embeddedInvoice"]
                init.type = x["type"]
                init.courierStatus = x["courierStatus"]
                init.cfop = x["cfop"]
                init.restitutions = packages["restitutions"]
                init.volumes = x["volumes"]
                init.EnableInferItems = x["EnableInferItems"]
        except:
            init.cache = 2
            
    
        try:
            dim_invoiceData = Fjson["invoiceData"]
            init.invoice_address = dim_invoiceData["address"]
            init.userPaymentInfo = dim_invoiceData["userPaymentInfo"]
        except:
            init.cache = 2
        
        try:    
            init.isActive = transactions["isActive"]
            init.transactionId = transactions["transactionId"]
            init.merchantName = transactions["merchantName"]
        except:
            init.cache = 2
            
        try:
            init.cancellationData = Fjson["cancellationData"]
            init.RequestedByUser = cancellationData["RequestedByUser"]
            init.RequestedBySystem = cancellationData["RequestedBySystem"]
            init.RequestedBySellerNotification = cancellationData["RequestedBySellerNotification"]
            init.RequestedByPaymentNotification = cancellationData["RequestedByPaymentNotification"]
            init.Reason = cancellationData["Reason"]
        except:
            init.cache = 2
    
        df1 = pd.DataFrame({
            'orderId': str(id),
            'emailTracked': str(init.emailTracked),
            'approvedBy': str(init.approvedBy),
            'cancelledBy': str(init.cancelledBy),
            'cancelReason': str(init.cancelReason),
            'sequence': init.sequence,
            'marketplaceOrderId': str(init.marketplaceOrderId),
            'marketplaceServicesEndpoint': str(init.marketplaceServicesEndpoint),
            'sellerOrderId': str(init.sellerOrderId),
            'origin': str(init.origin),
            'affiliateId': str(init.affiliateId),
            'salesChannel': init.salesChannel,
            'merchantName': str(init.merchantName),
            'status': str(init.status),
            'statusDescription': str(init.statusDescription),
            'value': init.value,
            'creationDate': init.creationDate,
            'lastChange': init.lastChange,
            'orderGroup': init.orderGroup,
            'giftRegistryData': str(init.giftRegistryData),
            'callCenterOperatorData': str(init.callCenterOperatorData),
            'followUpEmail': str(init.followUpEmail),
            'lastMessage': str(init.lastMessage),
            'hostname': str(init.hostname),
            'openTextField': str(init.openTextField),
            'roundingError': init.roundingError,
            'orderFormId': str(init.orderFormId),
            'commercialConditionData': str(init.commercialConditionData),
            'isCompleted': init.isCompleted,
            'customData': str(init.customData),
            'allowCancellation': init.allowCancellation,
            'allowEdition': init.allowEdition,
            'isCheckedIn': init.isCheckedIn,
            'authorizedDate': init.authorizedDate,
            'DIM_TOTAL_id_items': str(init.total_id_items),
            'DIM_TOTAL_name_items': str(init.total_name_items),
            'DIM_TOTAL_value_items': init.total_value_items,
            'DIM_TOTAL_id_discounts': str(init.total_id_discounts),
            'DIM_TOTAL_name_discounts': str(init.total_name_discounts),
            'DIM_TOTAL_value_discounts': init.total_value_discounts,
            'DIM_TOTAL_id_shipping': str(init.total_id_shipping),
            'DIM_TOTAL_name_shipping': str(init.total_name_shipping),
            'DIM_TOTAL_value_shipping': init.total_value_shipping,
            'DIM_TOTAL_id_tax': str(init.total_id_tax),
            'DIM_TOTAL_name_tax': str(init.total_name_tax),
            'DIM_TOTAL_value_tax': init.total_value_tax,
            'DIM_TOTAL_id_change': str(init.total_id_change),
            'DIM_TOTAL_name_change': str(init.total_name_change),
            'DIM_TOTAL_value_change': str(init.total_value_change),
            'DIM_ITEMS_uniqueId': str(init.items_uniqueId),
            'DIM_ITEMS_items_id': init.items_id,
            'DIM_ITEMS_productId': init.items_productId,
            'DIM_ITEMS_ean': init.items_ean,
            'DIM_ITEMS_lockId': str(init.items_lockId),
            'DIM_ITEMS_quantity': init.item_quantity,
            'DIM_ITEMS_seller': str(init.item_seller),
            'DIM_ITEMS_name': str(init.item_name),
            'DIM_ITEMS_refId': str(init.item_refId),
            'DIM_ITEMS_price': init.item_price,
            'DIM_ITEMS_listPrice': init.item_listPrice,
            'DIM_ITEMS_manualPrice': str(init.item_manualPrice),
            'DIM_ITEMS_imageUrl': str(init.item_imageUrl),
            'DIM_ITEMS_detailUrl': str(init.item_detailUrl),
            'DIM_ITEMS_sellerSku': init.item_sellerSku,
            'DIM_ITEMS_priceValidUntil': init.item_priceValidUntil,
            'DIM_ITEMS_commission': init.item_commission,
            'DIM_ITEMS_tax': init.item_tax,
            'DIM_ITEM_preSaleDate': str(init.item_preSaleDate),
            'DIM_ITEM_measurementUnit': str(init.item_measurementUnit),
            'DIM_ITEM_unitMultiplier': init.item_unitMultiplier,
            'DIM_ITEM_sellingPrice': init.item_sellingPrice,
            'DIM_ITEM_isGift': init.item_isGift,
            'DIM_ITEM_shippingPrice': str(init.item_shippingPrice),
            'DIM_ITEM_rewardValue': init.item_rewardValue,
            'DIM_ITEM_freightCommission': init.item_freightCommission,
            'DIM_ITEM_priceDefinition': str(init.item_price_definition),
            'DIM_ITEM_taxCode': str(init.item_taxCode),
            'DIM_ITEM_parentItemIndex': str(init.item_parentItemIndex),
            'DIM_ITEM_parentAssemblyBinding': str(init.item_parentAssemblyBinding),
            'DIM_ITEM_itemAttachment_name': str(init.item_itemAttachment_name),
            'DIM_ITEM_AInfo_brandName': str(init.brandName),
            'DIM_ITEM_AInfo_brandId': init.brandId,
            'DIM_ITEM_AInfo_categoriesIds': str(init.categoriesIds),
            'DIM_ITEM_AInfo_productClusterId': init.productClusterId,
            'DIM_ITEM_AInfo_commercialConditionId': init.commercialConditionId,
            'DIM_ITEM_AInfo_offeringInfo': str(init.offeringInfo),
            'DIM_ITEM_AInfo_offeringType': str(init.offeringType),
            'DIM_ITEM_AInfo_offeringTypeId': str(init.offeringTypeId),
            'DIM_ITEM_AInfo_cubicweight': init.cubicweight,
            'DIM_ITEM_AInfo_dim_height': init.height,
            'DIM_ITEM_AInfo_dim_length': init.length,
            'DIM_ITEM_AInfo_dim_weight': init.weight,
            'DIM_ITEM_AInfo_dim_width': init.width,
            'DIM_ITEM_calculatedSellingPrice': str(init.calculatedSellingPrice),
            'DIM_ITEM_priceDefinition_total': str(init.total),
            'DIM_CLIENT': str(init.client_id),
            'DIM_CLIENT_email': str(client_email),
            'DIM_CLIENT_firstName': str(init.client_firstName),
            'DIM_CLIENT_lastName': str(init.client_lastName),
            'DIM_CLIENT_documentType': str(init.client_documentType),
            'DIM_CLIENT_document': init.client_document,
            'DIM_CLIENT_phone': init.client_phone,
            'DIM_CLIENT_corporateName': str(init.client_corporateName),
            'DIM_CLIENT_tradeName': str(init.client_tradeName),
            'DIM_CLIENT_corporateDocument': str(init.client_corporateDocument),
            'DIM_CLIENT_stateInscription': str(init.client_stateInscription),
            'DIM_CLIENT_corporatePhone': str(init.client_corporatePhone),
            'DIM_CLIENT_isCorporate': init.client_isCorporate,
            'DIM_CLIENT_userProfileId': str(init.client_userProfileId),
            'DIM_CLIENT_customerClass': str(init.client_customerClass),
            'id_ratesAndBenefits': str(init.id_ratesAndBenefits),
            'DIM_SHIPPING_DATA_shippingData_id': str(init.shippingData_id),
            'DIM_SHIPPING_DATA_addressType': str(init.addressType),
            'DIM_SHIPPING_DATA_receiverName': str(init.receiverName),
            'DIM_SHIPPING_DATA_addressId': str(init.addressId),
            'DIM_SHIPPING_DATA_postalCode': init.postalCode,
            'DIM_SHIPPING_DATA_city': str(init.city),
            'DIM_SHIPPING_DATA_state': str(init.state),
            'DIM_SHIPPING_DATA_country': str(init.country),
            'DIM_SHIPPING_DATA_street': str(init.street),
            'DIM_SHIPPING_DATA_number': str(init.number),
            'DIM_SHIPPING_DATA_neighborhood': str(init.neighborhood),
            'DIM_SHIPPING_DATA_complement': str(init.complement),
            'DIM_SHIPPING_DATA_reference': str(init.reference),
            'DIM_SHIPPING_DATA_deliveryChannel': str(init.deliveryChannel),
            'DIM_SHIPPING_DATA_addressId': str(init.addressId),
            'DIM_SHIPPING_DATA_polygonName': str(init.polygonName),
            'DIM_SHIPPING_DATA_itemIndex': str(init.itemIndex),
            'DIM_SHIPPING_DATA_selectedSla': str(init.selectedSla),
            'DIM_SHIPPING_DATA_lockTTL': str(init.lockTTL),
            'DIM_SHIPPING_DATA_price': init.price,
            'DIM_SHIPPING_DATA_listPrice': init.listPrice,
            'DIM_SHIPPING_DATA_sellingPrice': init.sellingPrice,
            'DIM_SHIPPING_DATA_deliveryCompany': str(init.deliveryCompany),
            'DIM_SHIPPING_DATA_shippingEstimate': str(init.shippingEstimate),
            'DIM_SHIPPING_DATA_shippingEstimateDate': init.shippingEstimateDate,
            'DIM_SHIPPING_DATA_slas_id': str(init.slas_id),
            'DIM_SHIPPING_DATA_slas_name': str(init.slas_name),
            'DIM_SHIPPING_DATA_slas_shippingEstimate': str(init.slas_shippingEstimate),
            'DIM_SHIPPING_DATA_slas_price': init.slas_price,
            'DIM_SHIPPING_DATA_slas_deliveryChannel': str(init.slas_deliveryChannel),
            'DIM_SHIPPING_DATA_slas_polygonName': str(init.slas_polygonName),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo': str(init.slas_pickupStoreInfo_additionalInfo),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId': str(init.slas_pickupStoreInfo_dockId),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName': str(init.slas_pickupStoreInfo_friendlyName),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore': init.slas_pickupStoreInfo_isPickupStore,
            'DIM_SHIPPING_DATA_slas_id_01': str(init.slas_id_01),
            'DIM_SHIPPING_DATA_slas_name_01': str(init.slas_name_01),
            'DIM_SHIPPING_DATA_slas_shippingEstimate_01': str(init.slas_shippingEstimate_01),
            'DIM_SHIPPING_DATA_slas_price_01': init.slas_price_01,
            'DIM_SHIPPING_DATA_slas_deliveryChannel_01': init.slas_deliveryChannel_01,
            'DIM_SHIPPING_DATA_slas_polygonName_01': str(init.slas_polygonName_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_01': str(init.slas_pickupStoreInfo_additionalInfo_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_01': str(init.slas_pickupStoreInfo_dockId_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_01': str(init.slas_pickupStoreInfo_friendlyName_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_01': init.slas_pickupStoreInfo_isPickupStore_01,
            'DIM_SHIPPING_DATA_slas_id_02': str(init.slas_id_02),
            'DIM_SHIPPING_DATA_slas_name_02': str(init.slas_name_02),
            'DIM_SHIPPING_DATA_slas_shippingEstimate_02': str(init.slas_shippingEstimate_02),
            'DIM_SHIPPING_DATA_slas_deliveryWindow_02': str(init.slas_deliveryWindow_02),
            'DIM_SHIPPING_DATA_slas_price_02': init.slas_price_02,
            'DIM_SHIPPING_DATA_slas_deliveryChannel_02': str(init.slas_deliveryChannel_02),
            'DIM_SHIPPING_DATA_slas_polygonName_02': str(init.slas_polygonName_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_02': str(init.slas_pickupStoreInfo_additionalInfo_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_02': str(init.slas_pickupStoreInfo_dockId_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_02': str(init.slas_pickupStoreInfo_friendlyName_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_02': init.slas_pickupStoreInfo_isPickupStore_02,
            'DIM_SHIPPING_DATA_slas_id_03': str(init.slas_id_03),
            'DIM_SHIPPING_DATA_slas_name_03': str(init.slas_name_03),
            'DIM_SHIPPING_DATA_slas_shippingEstimate_03': str(init.slas_shippingEstimate_03),
            'DIM_SHIPPING_DATA_slas_price_03': init.slas_price_03,
            'DIM_SHIPPING_DATA_slas_deliveryChannel_03': str(init.slas_deliveryChannel_03),
            'DIM_SHIPPING_DATA_slas_polygonName_03': str(init.slas_polygonName_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_03': str(init.slas_pickupStoreInfo_additionalInfo_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_03': str(init.slas_pickupStoreInfo_dockId_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_03': str(init.slas_pickupStoreInfo_friendlyName_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_03': init.slas_pickupStoreInfo_isPickupStore_03,
            'DIM_SHIPPING_DATA_courierId_delivery': str(init.courierId),
            'DIM_SHIPPING_DATA_courierName_delivery': str(init.courierName),
            'DIM_SHIPPING_DATA_dockId_delivery': init.dockId,
            'DIM_SHIPPING_DATA_quantity_delivery': init.quantity,
            'DIM_SHIPPING_DATA_warehouseId': str(init.warehouseId),
            'DIM_SHIPPING_DATA_pickupStoreInfo_additionalInfo': str(init.pickupStoreInfo_additionalInfo),
            'DIM_SHIPPING_DATA_pickupStoreInfo_dockId': str(init.pickupStoreInfo_dockId),
            'DIM_SHIPPING_DATA_pickupStoreInfo_friendlyName': str(init.pickupStoreInfo_friendlyName),
            'DIM_SHIPPING_DATA_pickupStoreInfo_isPickupStore': init.pickupStoreInfo_isPickupStore,
            'DIM_SHIPPING_DATA_selectedAddresses_addressId': str(init.selectedAddresses_addressId),
            'DIM_SHIPPING_DATA_selectedAddresses_addressType': str(init.selectedAddresses_addressType),
            'DIM_SHIPPING_DATA_selectedAddresses_receiverName': init.selectedAddresses_receiverName,
            'DIM_SHIPPING_DATA_selectedAddresses_street': str(init.selectedAddresses_street),
            'DIM_SHIPPING_DATA_selectedAddresses_number': str(init.selectedAddresses_number),
            'DIM_SHIPPING_DATA_selectedAddresses_complement': str(init.selectedAddresses_complement),
            'DIM_SHIPPING_DATA_selectedAddresses_neighborhood': str(init.selectedAddresses_neighborhood),
            'DIM_SHIPPING_DATA_selectedAddresses_postalCode': init.selectedAddresses_postalCode,
            'DIM_SHIPPING_DATA_selectedAddresses_city': str(init.selectedAddresses_city),
            'DIM_SHIPPING_DATA_selectedAddresses_state': str(init.selectedAddresses_state),
            'DIM_SHIPPING_DATA_selectedAddresses_country': str(init.selectedAddresses_country),
            'DIM_SHIPPING_DATA_selectedAddresses_reference': str(init.selectedAddresses_reference),
            'transactions_isActive': init.transactions_isActive,
            'transactions_transactionId': str(init.transactions_transactionId),
            'transactions_merchantName': str(init.transactions_merchantName),
            'payments_id': str(init.payments_id),
            'payments_paymentSystem': init.payments_paymentSystem,
            'payments_paymentSystemName': str(init.payments_paymentSystemName),
            'payments_value': init.payments_value,
            'payments_installments': init.payments_installments,
            'payments_referenceValue': init.payments_referenceValue,
            'payments_cardHolder': str(init.payments_cardHolder),
            'payments_firstDigits': init.payments_firstDigits,
            'payments_lastDigits': init.payments_lastDigits,
            'payments_url': str(init.payments_url),
            'payments_giftCardId': str(init.payments_giftCardId),
            'payments_giftCardName': str(init.payments_giftCardName),
            'payments_giftCardCaption': str(init.payments_giftCardCaption),
            'payments_redemptionCode': str(init.payments_redemptionCode),
            'payments_group': str(init.payments_group),
            'payments_dueDate': str(init.payments_dueDate),
            'payments_cardNumber': str(init.payments_cardNumber),
            'payments_cvv2': str(init.payments_cvv2),
            'payments_expireMonth': str(init.payments_expireMonth),
            'payments_expireYear': str(init.payments_expireYear),
            'payments_giftCardProvider': str(init.payments_giftCardProvider),
            'payments_giftCardAsDiscount': str(init.payments_giftCardAsDiscount),
            'payments_koinUrl': str(init.payments_koinUrl),
            'payments_accountId': str(init.payments_accountId),
            'payments_parentAccountId': str(init.payments_parentAccountId),
            'payments_bankIssuedInvoiceIdentificationNumber': str(init.payments_bankIssuedInvoiceIdentificationNumber),
            'payments_bankIssuedInvoiceIdentificationNumberFormatted': str(init.payments_bankIssuedInvoiceIdentificationNumberFormatted),
            'payments_bankIssuedInvoiceBarCodeNumber': str(init.payments_bankIssuedInvoiceBarCodeNumber),
            'payments_bankIssuedInvoiceBarCodeType': str(init.payments_bankIssuedInvoiceBarCodeType),
            'payments_Tid': str(init.payments_Tid),
            'payments_ReturnCode': init.payments_ReturnCode,
            'payments_Message': str(init.payments_Message),
            'payments_authId': str(init.payments_authId),
            'payments_acquirer': str(init.payments_acquirer),
            'billingAddress_postalCode': str(init.billingAddress_postalCode),
            'billingAddress_city': str(init.billingAddress_city),
            'billingAddress_state': str(init.billingAddress_state),
            'billingAddress_country': str(init.billingAddress_country),
            'billingAddress_street': str(init.billingAddress_street),
            'billingAddress_number': init.billingAddress_number,
            'billingAddress_neighborhood': str(init.billingAddress_neighborhood),
            'billingAddress_complement': str(init.billingAddress_complement),
            'billingAddress_reference': str(init.billingAddress_reference),
            'seller_id': str(init.seller_id),
            'seller_name': str(init.seller_name),
            'seller_logo': str(init.seller_logo),
            'changesAttachment_id': str(init.changesAttachment_id),
            'storePreferencesData_countryCode': str(init.storePreferencesData_countryCode),
            'storePreferencesData_currencyCode': str(init.storePreferencesData_currencyCode),
            'storePreferencesData_currencyLocale': init.storePreferencesData_currencyLocale,
            'storePreferencesData_currencySymbol': str(init.storePreferencesData_currencySymbol),
            'storePreferencesData_timeZone': str(init.storePreferencesData_timeZone),
            'CurrencyDecimalDigits': init.CurrencyDecimalDigits,
            'CurrencyDecimalSeparator': str(init.CurrencyDecimalSeparator),
            'CurrencyGroupSeparator': str(init.CurrencyGroupSeparator),
            'CurrencyGroupSize': init.CurrencyGroupSize,
            'StartsWithCurrencySymbol': init.StartsWithCurrencySymbol,
            'marketplace_baseURL': str(init.baseURL),
            'marketplace_isCertified': str(init.isCertified),
            'marketplace_name': str(init.name),
            'itemMetadata_Id': init.itemMetadata_Id,
            'itemMetadata_Seller': str(init.itemMetadata_Seller),
            'itemMetadata_Name': str(init.itemMetadata_Name),
            'itemMetadata_SkuName': str(init.itemMetadata_SkuName),
            'itemMetadata_ProductId': init.itemMetadata_ProductId,
            'itemMetadata_RefId': init.itemMetadata_RefId,
            'itemMetadata_Ean': init.itemMetadata_Ean,
            'itemMetadata_ImageUrl': str(init.itemMetadata_ImageUrl),
            'itemMetadata_DetailUrl': str(init.itemMetadata_DetailUrl),
            'subscriptionData': str(init.subscriptionData),
            'taxData': str(init.taxData),
            'courier': str(init.courier),
            'invoiceNumber': str(init.invoiceNumber),
            'invoiceValue': str(init.invoiceValue),
            'invoiceUrl': str(init.invoiceUrl),
            'issuanceDate': init.issuanceDate,
            'trackingNumber': str(init.trackingNumber),
            'invoiceKey': str(init.invoiceKey),
            'trackingUrl': str(init.trackingUrl),
            'embeddedInvoice': init.embeddedInvoice,
            'type': str(init.type),
            'cfop': str(init.cfop),
            'restitutions': str(init.restitutions),
            'volumes': str(init.volumes),
            'EnableInferItems': str(init.EnableInferItems),
            'invoice_address': str(init.invoice_address),
            'userPaymentInfo': str(init.userPaymentInfo),
            'serialNumbers':str(init.item_serialNumbers),
            'isActive':init.isActive,
            'transactionId':str(init.transactionId),
            'merchantName':str(init.merchantName),
            'RequestedByUser':str(init.RequestedByUser),
            'RequestedBySystem':str(init.RequestedBySystem),
            'RequestedBySellerNotification':str(init.RequestedBySellerNotification),
            'RequestedByPaymentNotification':str(init.RequestedByPaymentNotification),
            'Reason':str(init.Reason),
            'CancellationDate':str(init.CancellationDate),
            'invoicedDate': str(init.invoicedDate)}, index=[0])
        init.df = init.df.append(df1)
    except:
        init.cache = 2

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_vtex_order` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_vtex_order`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def run():
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    table_schema = {
        "name": "CancellationDate",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "Reason",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "RequestedBySystem",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "isActive",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "RequestedBySellerNotification",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "userPaymentInfo",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "EnableInferItems",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "volumes",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "restitutions",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "type",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "embeddedInvoice",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "trackingUrl",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "issuanceDate",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "invoiceUrl",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "courier",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "subscriptionData",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "itemMetadata_DetailUrl",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "itemMetadata_ProductId",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "itemMetadata_Id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "marketplace_name",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "marketplace_isCertified",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "marketplace_baseURL",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "CurrencyDecimalSeparator",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "CurrencyDecimalDigits",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "storePreferencesData_timeZone",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "storePreferencesData_currencySymbol",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "storePreferencesData_currencyCode",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "storePreferencesData_countryCode",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "RequestedByUser",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "seller_logo",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "seller_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "billingAddress_reference",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "billingAddress_complement",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "billingAddress_neighborhood",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "billingAddress_number",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "billingAddress_street",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "billingAddress_country",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "billingAddress_state",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "billingAddress_city",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_acquirer",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "itemMetadata_Name",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_authId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_Message",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "itemMetadata_RefId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "storePreferencesData_currencyLocale",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "payments_ReturnCode",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_Tid",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_bankIssuedInvoiceBarCodeType",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_bankIssuedInvoiceBarCodeNumber",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "itemMetadata_Seller",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_bankIssuedInvoiceIdentificationNumberFormatted",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_bankIssuedInvoiceIdentificationNumber",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_parentAccountId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_giftCardProvider",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_expireMonth",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_cvv2",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_group",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_redemptionCode",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_giftCardCaption",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_giftCardName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_giftCardId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_url",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_lastDigits",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "invoicedDate",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "itemMetadata_ImageUrl",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_firstDigits",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "payments_value",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "payments_paymentSystemName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_paymentSystem",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "payments_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "transactions_merchantName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedAddresses_postalCode",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "origin",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedAddresses_complement",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedAddresses_number",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedAddresses_street",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedAddresses_addressType",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_listPrice",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedAddresses_addressId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_01",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_02",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_pickupStoreInfo_friendlyName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_dockId_delivery",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_courierName_delivery",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_courierId_delivery",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "invoiceNumber",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_03",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_price_03",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_name_03",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "authorizedDate",
        "type": "TIMESTAMP",
        "mode": "NULLABLE"
    },{
        "name": "sequence",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_email",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_02",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_shippingEstimate_03",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "salesChannel",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_id_01",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_polygonName_02",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_accountId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_deliveryWindow_02",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_name_02",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_stateInscription",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_01",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_referenceValue",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_productClusterId",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_pickupStoreInfo_additionalInfo",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_giftCardAsDiscount",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_polygonName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_quantity_delivery",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_deliveryChannel_01",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_value_shipping",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_price_01",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "CurrencyGroupSeparator",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_isCorporate",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_polygonName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_deliveryChannel",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_price",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_shippingPrice",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_shippingEstimate",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_name",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "trackingNumber",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "customData",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_sellingPrice",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_itemIndex",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_rewardValue",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_deliveryChannel",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_parentAssemblyBinding",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_complement",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_neighborhood",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_number",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_id_03",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "transactions_isActive",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_dim_length",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_shippingData_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_country",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "cancelReason",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_state",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "transactions_transactionId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedAddresses_reference",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_postalCode",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_addressId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_customerClass",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "RequestedByPaymentNotification",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_userProfileId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedAddresses_receiverName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_phone",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_02",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_corporatePhone",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "invoiceValue",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_corporateDocument",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_tradeName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_shippingEstimate",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "emailTracked",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_name_tax",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_corporateName",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_documentType",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_03",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_firstName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_calculatedSellingPrice",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_warehouseId",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_dim_width",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_city",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "payments_cardNumber",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_productId",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_dim_height",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "transactionId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "merchantName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_taxCode",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_refId",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_commercialConditionId",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_itemAttachment_name",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_offeringType",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "taxData",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "hostname",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_offeringTypeId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_pickupStoreInfo_isPickupStore",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_document",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_id_02",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_priceDefinition_total",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "statusDescription",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_lockId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_receiverName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_03",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "billingAddress_postalCode",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_quantity",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_deliveryChannel_03",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_reference",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_01",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "roundingError",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_dim_weight",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_isGift",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_deliveryChannel_02",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_02",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_tax",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "payments_installments",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "value",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_pickupStoreInfo_dockId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_sellerSku",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "changesAttachment_id",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_listPrice",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "lastMessage",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_name",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "itemMetadata_Ean",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_detailUrl",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_seller",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_manualPrice",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_price",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "creationDate",
        "type": "DATE",
        "mode": "NULLABLE"
    },{
        "name": "payments_dueDate",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_ean",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_commission",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "openTextField",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedAddresses_city",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_measurementUnit",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "approvedBy",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_uniqueId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_name_01",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_addressType",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_id_change",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_value_tax",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedSla",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "followUpEmail",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "invoice_address",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_sellingPrice",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_id_tax",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_name_shipping",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_id_items",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "marketplaceServicesEndpoint",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_street",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_id_shipping",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_unitMultiplier",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_value_discounts",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "invoiceKey",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "orderGroup",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "payments_koinUrl",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_name_discounts",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_price_02",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "serialNumbers",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_name_change",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_id_discounts",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_name_items",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_deliveryCompany",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_preSaleDate",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_items_id",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "CurrencyGroupSize",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "cancelledBy",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "allowEdition",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_value_change",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_categoriesIds",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_brandId",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "allowCancellation",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_01",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "cfop",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "commercialConditionData",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "status",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "orderFormId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_polygonName_01",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_shippingEstimate_01",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "isCheckedIn",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_imageUrl",
        "type": "DATE",
        "mode": "NULLABLE"
    },{
        "name": "payments_cardHolder",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedAddresses_neighborhood",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "isCompleted",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_polygonName_03",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "payments_expireYear",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "sellerOrderId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_brandName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "id_ratesAndBenefits",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_TOTAL_value_items",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "giftRegistryData",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_03",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "lastChange",
        "type": "DATE",
        "mode": "NULLABLE"
    },{
        "name": "callCenterOperatorData",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_cubicweight",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedAddresses_state",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEMS_priceValidUntil",
        "type": "DATE",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_AInfo_offeringInfo",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "marketplaceOrderId",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "itemMetadata_SkuName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "StartsWithCurrencySymbol",
        "type": "BOOLEAN",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_selectedAddresses_country",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_CLIENT_lastName",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_slas_shippingEstimate_02",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_lockTTL",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_parentItemIndex",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_price",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "DIM_ITEM_freightCommission",
        "type": "INTEGER",
        "mode": "NULLABLE"
    },{
        "name": "affiliateId",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },{
        "name": "seller_name",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "DIM_SHIPPING_DATA_shippingEstimateDate",
        "type": "DATE",
        "mode": "NULLABLE"
    },{
        "name": "orderId",
        "type": "STRING",
        "mode": "NULLABLE"
    }
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_vtex_order'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    #job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.schema = format_schema(table_schema)
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()        


def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.test.shopstar_vtex_order_`))')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId,registro)
        if registro == 5:
            run()
        if registro == 100:
            run()
        if registro == 5000:
            run()
        if registro == 10000:
            run()
        if registro == 20000:
            run()
        if registro == 30000:
            run()
        if registro == 40000:
            run()
        if registro == 50000:
            run()
        if registro == 60000:
            run()
        if registro == 70000:
            run()
    run()
        
    
get_params()