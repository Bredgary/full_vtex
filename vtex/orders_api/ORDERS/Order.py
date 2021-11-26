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
    emailTracked = None
    approvedBy = None
    cancelledBy = None
    cancelReason = None
    orderId = None
    sequence = None
    marketplaceOrderId = None
    marketplaceServicesEndpoint = None
    sellerOrderId = None
    origin = None
    affiliateId = None
    salesChannel = None
    merchantName = None
    status = None
    statusDescription = None
    value = None
    creationDate = None
    lastChange = None
    orderGroup = None
    giftRegistryData = None
    marketingData = None
    callCenterOperatorData = None
    followUpEmail = None
    lastMessage = None
    hostname = None
    invoiceData = None
    openTextField = None
    roundingError = None
    orderFormId = None
    commercialConditionData = None
    isCompleted = None
    customData = None
    allowCancellation = None
    allowEdition = None
    isCheckedIn = None
    authorizedDate = None
    invoicedDate = None
    
    '''
    Dimensiones TOTAL
    '''
    
    total_id_items = None
    total_name_items = None
    total_value_items = None
    total_id_discounts = None
    total_name_discounts = None
    total_value_discounts = None
    total_id_shipping = None
    total_name_shipping = None
    total_value_shipping = None
    total_id_tax = None
    total_name_tax = None
    total_value_tax = None
    total_id_change = None
    total_name_change = None
    total_value_change = None

    '''
    Dimensiones ITEMS
    '''

    items_uniqueId = None
    items_id = None
    items_productId = None
    items_ean = None
    items_lockId = None
    item_quantity = None
    item_seller = None
    item_name = None
    item_refId = None
    item_price = None
    item_listPrice = None
    item_manualPrice = None
    item_imageUrl = None
    item_detailUrl = None
    item_sellerSku = None
    item_priceValidUntil = None
    item_commission = None
    item_tax = None
    item_preSaleDate = None
    item_itemAttachment_name = None
    item_measurementUnit = None
    item_unitMultiplier = None
    item_sellingPrice = None
    item_isGift = None
    item_shippingPrice = None
    item_rewardValue = None
    item_freightCommission = None
    item_taxCode = None
    item_parentItemIndex = None
    item_parentAssemblyBinding = None
    
    '''
    Dimensiones ITEMS_INFORMATION__ADITIONAL
    '''
    brandName = None
    brandId = None
    categoriesIds = None
    productClusterId = None
    commercialConditionId = None
    offeringInfo = None
    offeringType = None
    offeringTypeId = None
    '''
    Dimensiones ITEMS_INFORMATION__ADITIONAL_dimension
    '''
    cubicweight = None
    height = None
    length = None
    weight = None
    width = None
    '''
    Dimensiones ITEMS_priceDefinition
    '''
    total = None
    calculatedSellingPrice = None
    
    '''
    clientProfileData
    '''
    
    client_id = None
    client_email = None
    client_firstName = None
    client_lastName = None
    client_documentType = None
    client_document = None
    client_phone = None
    client_corporateName = None
    client_tradeName = None
    client_corporateDocument = None
    client_stateInscription = None
    client_corporatePhone = None
    client_isCorporate = None
    client_userProfileId = None
    client_customerClass = None
    
    '''
    "ratesAndBenefitsData"
    '''
    
    id_ratesAndBenefits = None
    
    '''
    "shippingData"
    '''
    
    shippingData_id = None
    
    '''
    address
    '''
    
    addressType = None
    receiverName = None
    addressId = None
    postalCode = None
    city = None
    state = None
    country = None
    street = None
    number = None
    neighborhood = None
    complement = None
    reference = None
    
    '''
    logisticsInfo
    '''
    deliveryChannel = None
    trackingHints = None
    addressId = None
    polygonName = None
    itemIndex = None
    selectedSla = None
    lockTTL = None
    price = None
    listPrice = None
    sellingPrice = None
    deliveryWindow = None
    deliveryCompany = None
    shippingEstimate = None
    shippingEstimateDate = None
    
    '''
    slas
    '''
    slas_id = None
    slas_name = None
    slas_shippingEstimate = None
    slas_deliveryWindow = None
    slas_price = None
    slas_deliveryChannel = None
    slas_polygonName = None
    '''
    slas_pickupStoreInfo
    '''
    slas_pickupStoreInfo_additionalInfo = None
    slas_pickupStoreInfo_address = None
    slas_pickupStoreInfo_dockId = None
    slas_pickupStoreInfo_friendlyName = None
    slas_pickupStoreInfo_isPickupStore = None
    
    '''
    slas_1
    '''
    slas_id_01 = None
    slas_name_01 = None
    slas_shippingEstimate_01 = None
    slas_deliveryWindow_01 = None
    slas_price_01 = None
    slas_deliveryChannel_01 = None
    slas_polygonName_01 = None
    '''
    slas_pickupStoreInfo_1
    '''
    slas_pickupStoreInfo_additionalInfo_01 = None
    slas_pickupStoreInfo_address_01 = None
    slas_pickupStoreInfo_dockId_01 = None
    slas_pickupStoreInfo_friendlyName_01 = None
    slas_pickupStoreInfo_isPickupStore_01 = None
    
    '''
    slas_2
    '''
    slas_id_02 = None
    slas_name_02 = None
    slas_shippingEstimate_02 = None
    slas_deliveryWindow_02 = None
    slas_price_02 = None
    slas_deliveryChannel_02 = None
    slas_polygonName_02 = None
    '''
    slas_pickupStoreInfo_2
    '''
    slas_pickupStoreInfo_additionalInfo_02 = None
    slas_pickupStoreInfo_address_02 = None
    slas_pickupStoreInfo_dockId_02 = None
    slas_pickupStoreInfo_friendlyName_02 = None
    slas_pickupStoreInfo_isPickupStore_02 = None
    
    '''
    slas_3
    '''
    slas_id_03 = None
    slas_name_03 = None
    slas_shippingEstimate_03 = None
    slas_deliveryWindow_03 = None
    slas_price_03 = None
    slas_deliveryChannel_03 = None
    slas_polygonName_03 = None
    '''
    slas_pickupStoreInfo_3
    '''
    slas_pickupStoreInfo_additionalInfo_03 = None
    slas_pickupStoreInfo_address_03 = None
    slas_pickupStoreInfo_dockId_03 = None
    slas_pickupStoreInfo_friendlyName_03 = None
    slas_pickupStoreInfo_isPickupStore_03 = None
    
    '''
    deliveryIds
    '''
    courierId = None
    courierName = None
    dockId = None
    quantity = None
    warehouseId = None
    
    '''
    pickupStoreInfo
    '''
    pickupStoreInfo_additionalInfo = None
    pickupStoreInfo_address = None
    pickupStoreInfo_dockId = None
    pickupStoreInfo_friendlyName = None
    pickupStoreInfo_isPickupStore = None
    
    '''
    selectedAddresses
    '''
    
    selectedAddresses_addressId = None
    selectedAddresses_addressType = None
    selectedAddresses_receiverName = None
    selectedAddresses_street = None
    selectedAddresses_number = None
    selectedAddresses_complement = None
    selectedAddresses_neighborhood = None
    selectedAddresses_postalCode = None
    selectedAddresses_city = None
    selectedAddresses_state = None
    selectedAddresses_country = None
    selectedAddresses_reference = None
    
    '''
    transactions
    '''
    transactions_isActive = None
    transactions_transactionId = None
    transactions_merchantName = None
    
    '''
    payments
    '''
    
    payments_id = None
    payments_paymentSystem = None
    payments_paymentSystemName = None
    payments_value = None
    payments_installments = None
    payments_referenceValue = None
    payments_cardHolder = None
    payments_firstDigits = None
    payments_lastDigits = None
    payments_url = None
    payments_giftCardId = None
    payments_giftCardName = None
    payments_giftCardCaption = None
    payments_redemptionCode = None
    payments_group = None
    payments_tid = None
    payments_dueDate = None

    '''
    billingAddress
    '''
    
    billingAddress_postalCode = None
    billingAddress_city = None
    billingAddress_state = None
    billingAddress_country = None
    billingAddress_street = None
    billingAddress_number = None
    billingAddress_neighborhood = None
    billingAddress_complement = None
    billingAddress_reference = None
    
    '''
    Sellers
    '''
    seller_id = None
    seller_name = None
    seller_logo = None
    
    '''
    changesAttachment
    '''
    changesAttachment_id = None
    
    '''
    storePreferencesData
    '''
    storePreferencesData_countryCode = None
    storePreferencesData_currencyCode = None
    storePreferencesData_currencyLocale = None
    storePreferencesData_currencySymbol = None
    storePreferencesData_timeZone = None
    
    '''
    currencyFormatInfo
    '''
    
    CurrencyDecimalDigits = None
    CurrencyDecimalSeparator = None
    CurrencyGroupSeparator = None
    CurrencyGroupSize = None
    StartsWithCurrencySymbol = None
    
    '''
    currencyFormatInfo
    '''
    
    baseURL = None
    isCertified = None
    name = None
    
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
        print("No se pudo desencriptar Email: "+str(email))
     
def get_order(id,reg):
    url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
    response = requests.request("GET", url, headers=init.headers)
    Fjson = json.loads(response.text)
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
    if "marketplaceServicesEndpoint" in Fjson:
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
    INIT DIMENSION
    '''
    try:
        Total = Fjson["totals"]
    except:
        print("Total. No tiene datos")
    try:
        clientProfileData = Fjson["clientProfileData"]
    except:
        print("clientProfileData. No tiene datos")
    try:
        marketplace = Fjson["marketplace"]
    except:
        print("marketplace. No tiene datos")
    try:
        ratesAndBenefitsData = Fjson["ratesAndBenefitsData"]
    except:
        print("ratesAndBenefitsData. No tiene datos")
    try:
        storePreferencesData = Fjson["storePreferencesData"]
    except:
        print("storePreferencesData. No tiene datos")
    try:
        currencyFormatInfo = storePreferencesData["currencyFormatInfo"]
    except:
        print("currencyFormatInfo. No tiene datos") 
    try:
        shippingData = Fjson["shippingData"]
    except:
        print("shippingData. No tiene datos")
    try:
        logisticsInfo_0 = shippingData["logisticsInfo"]
    except:
        print("logisticsInfo_0. No tiene datos")  
    try:
        selectedAddresses_ = shippingData["selectedAddresses"]
    except:
        print("selectedAddresses_. No tiene datos")  
    try:
        selectedAddresses = selectedAddresses_[0]
    except:
        print("selectedAddresses. No tiene datos") 
    try:
        logisticsInfo = logisticsInfo_0[0]
    except:
        print("logisticsInfo. No tiene datos")
    try:
        address = shippingData["address"]
    except:
        print("address. No tiene datos")
    try:
        slas = logisticsInfo["slas"]
    except:
        print("slas. No tiene datos")
    try:
        deliveryIds_ = logisticsInfo["deliveryIds"]
    except:
        print("deliveryIds_. No tiene datos")
    try:
        deliveryIds = deliveryIds_[0]
    except:
        print("deliveryIds. No tiene datos")
    try:
        pickupStoreInfo = logisticsInfo["pickupStoreInfo"]
    except:
        print("pickupStoreInfo. No tiene datos")
    try:
        slas_0 = slas[0]
        pickupStoreInfo = slas_0["pickupStoreInfo"]
    except:
        print("slas_0 No tiene datos")
    try:
        slas_1 = slas[1]
        pickupStoreInfo_1 = slas_1["pickupStoreInfo"]
    except:
        print("slas_1 No tiene datos")
    try:
        slas_2 = slas[2]
        pickupStoreInfo_2 = slas_2["pickupStoreInfo"]
    except:
        print("slas_2 No tiene datos")
    try:
        slas_3 = slas[3]
        pickupStoreInfo_3 = slas_3["pickupStoreInfo"]
    except:
        print("slas_3 No tiene datos")
    try:
        items = Fjson["items"]
    except:
        print("items No tiene datos")
    try:
        changesAttachment = Fjson["changesAttachment"]
    except:
        print("changesAttachment No tiene datos")
    try:
        paymentData = Fjson["paymentData"]
    except:
        print("paymentData No tiene datos")
    try:
        sellers_ = Fjson["sellers"]
    except:
        print("sellers_ No tiene datos")
    try:
        sellers = sellers_[0]
    except:
        print("sellers No tiene datos")
    try:
        transactions_ = paymentData["transactions"]
    except:
        print("transactions_ No tiene datos")
    try:
        transactions = transactions_[0]
    except:
        print("transactions No tiene datos")
    try:
        payments_ = transactions["payments"]
    except:
        print("payments_ No tiene datos")
    try:
        payments = payments_[0]
    except:
        print("payments No tiene datos")
    try:
        billingAddress = payments["billingAddress"]
    except:
        print("billingAddress No tiene datos")
    try:
        Items = items[0]
    except:
        print("Items No tiene datos")
    try:
        itemAttachment = Items["itemAttachment"]
    except:
        print("itemAttachment No tiene datos")
    try:
        additionalInfo = Items["additionalInfo"]
    except:
        print("additionalInfo No tiene datos")
    try:
        priceDefinition = Items["priceDefinition"]
    except:
        print("priceDefinition No tiene datos")
    try:
        sellingPrice = Items["sellingPrice"]
    except:
        print("sellingPrice No tiene datos")
    try:
        dimension = additionalInfo["dimension"]
    except:
        print("dimension No tiene datos")
    
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
        print("No hay datos items")
    try:
        if Total[1]:
            discounts = Total[1]
            init.total_id_discounts = discounts["id"]
            init.total_name_discounts = discounts["name"]
            init.total_value_discounts = discounts["value"]
    except:
        print("No hay datos discounts")
    try:
        if Total[2]:
            shipping = Total[2]
            init.total_id_shipping = shipping["id"]
            init.total_name_shipping = shipping["name"]
            init.total_value_shipping = shipping["value"]
    except:
        print("No hay datos shipping")
    try:
        if Total[3]:
            tax = Total[3]
            init.total_id_tax = tax["id"]
            init.total_name_tax = tax["name"]
            init.total_value_tax = tax["value"]
    except:
        print("No hay datos tax")
    try:
        if  Total[4]:
            change = Total[4]
            init.total_id_change = change["id"]
            init.total_name_change = change["name"]
            init.total_value_change = change["value"]
    except:
        print("No hay datos change")
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
    except:
        print("No hay datos Items")
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
        print("No hay datos. additionalInfo")
    try:
        init.cubicweight = dimension["cubicweight"]
        init.height = dimension["height"]
        init.length = dimension["length"]
        init.weight = dimension["weight"]
        init.width = dimension["width"]
    except:
        print("No hay datos. dimension")
    try:
        init.item_itemAttachment_name = itemAttachment["name"]
    except:
        print("No hay datos. itemAttachment")
    try:
        init.calculatedSellingPrice = priceDefinition["calculatedSellingPrice"]
        init.total = priceDefinition["total"]
    except:
        print("No hay datos priceDefinition")      
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
        
        client_email = init.client_email
        followUpEmail = init.followUpEmail
        client_email_01 = decrypt_email(client_email)
        followUpEmail_02 = decrypt_email(followUpEmail)   
    except:
        print("No se pudo cargar Client Profile") 
    try:
        init.id_ratesAndBenefits = ratesAndBenefitsData["id"]
    except:
        print("No se pudo cargar ratesAndBenefitsData")
    try:
        init.shippingData_id = shippingData["id"]
    except:
        print("No se pudo cargar shippingData")
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
        print("No se pudo cargar address")
        
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
        print("No se pudo cargar address")
    try:
        init.slas_id = slas_0["id"]
        init.slas_name = slas_0["name"]
        init.slas_shippingEstimate = slas_0["shippingEstimate"]
        init.slas_deliveryWindow = slas_0["deliveryWindow"]
        init.slas_price = slas_0["price"]
        init.slas_deliveryChannel = slas_0["deliveryChannel"]
        init.slas_polygonName = slas_0["polygonName"]
    except:
        print("No hay datos slas")
        
    try:
        init.slas_pickupStoreInfo_additionalInfo = pickupStoreInfo["additionalInfo"]
        init.slas_pickupStoreInfo_address = pickupStoreInfo["address"]
        init.slas_pickupStoreInfo_dockId = pickupStoreInfo["dockId"]
        init.slas_pickupStoreInfo_friendlyName = pickupStoreInfo["friendlyName"]
        init.slas_pickupStoreInfo_isPickupStore = pickupStoreInfo["isPickupStore"]
    except:
        print("No hay datos pickupStoreInfo")
    try:
        init.slas_id_01 = slas_1["id"]
        init.slas_name_01 = slas_1["name"]
        init.slas_shippingEstimate_01 = slas_1["shippingEstimate"]
        init.slas_deliveryWindow_01 = slas_1["deliveryWindow"]
        init.slas_price_01 = slas_1["price"]
        init.slas_deliveryChannel_01 = slas_1["deliveryChannel"]
        init.slas_polygonName_01 = slas_1["polygonName"]
    except:
        print("No hay datos slas_1")
    try:
        init.slas_pickupStoreInfo_additionalInfo_01 = pickupStoreInfo_1["additionalInfo"]
        init.slas_pickupStoreInfo_address_01 = pickupStoreInfo_1["address"]
        init.slas_pickupStoreInfo_dockId_01 = pickupStoreInfo_1["dockId"]
        init.slas_pickupStoreInfo_friendlyName_01 = pickupStoreInfo_1["friendlyName"]
        init.slas_pickupStoreInfo_isPickupStore_01 = pickupStoreInfo_1["isPickupStore"]
    except:
        print("No hay datos pickupStoreInfo_1")
    
    try:
        init.slas_id_02 = slas_2["id"]
        init.slas_name_02 = slas_2["name"]
        init.slas_shippingEstimate_02 = slas_2["shippingEstimate"]
        init.slas_deliveryWindow_02 = slas_2["deliveryWindow"]
        init.slas_price_02 = slas_2["price"]
        init.slas_deliveryChannel_02 = slas_2["deliveryChannel"]
        init.slas_polygonName_02 = slas_2["polygonName"]
    except:
        print("No hay datos slas_2")
    try:
        init.slas_pickupStoreInfo_additionalInfo_02 = pickupStoreInfo_2["additionalInfo"]
        init.slas_pickupStoreInfo_address_02 = pickupStoreInfo_2["address"]
        init.slas_pickupStoreInfo_dockId_02 = pickupStoreInfo_2["dockId"]
        init.slas_pickupStoreInfo_friendlyName_02 = pickupStoreInfo_2["friendlyName"]
        init.slas_pickupStoreInfo_isPickupStore_02 = pickupStoreInfo_2["isPickupStore"]
    except:
        print("No hay datos pickupStoreInfo_2")
        
    try:
        init.slas_id_03 = slas_3["id"]
        init.slas_name_03 = slas_3["name"]
        init.slas_shippingEstimate_03 = slas_3["shippingEstimate"]
        init.slas_deliveryWindow_03 = slas_3["deliveryWindow"]
        init.slas_price_03 = slas_3["price"]
        init.slas_deliveryChannel_03 = slas_3["deliveryChannel"]
        init.slas_polygonName_03 = slas_3["polygonName"]
    except:
        print("No hay datos slas_3")
    try: 
        init.slas_pickupStoreInfo_additionalInfo_03 = pickupStoreInfo_3["additionalInfo"]
        init.slas_pickupStoreInfo_address_03 = pickupStoreInfo_3["address"]
        init.slas_pickupStoreInfo_dockId_03 = pickupStoreInfo_3["dockId"]
        init.slas_pickupStoreInfo_friendlyName_03 = pickupStoreInfo_3["friendlyName"]
        init.slas_pickupStoreInfo_isPickupStore_03 = pickupStoreInfo_3["isPickupStore"]
    except:
        print("No hay datos pickupStoreInfo_3")
    try:
        init.courierId = deliveryIds["courierId"]
        init.courierName = deliveryIds["courierName"]
        init.dockId = deliveryIds["dockId"]
        init.quantity = deliveryIds["quantity"]
        init.warehouseId = deliveryIds["warehouseId"]
    except:
        print("No hay datos deliveryIds")
    try: 
        init.pickupStoreInfo_additionalInfo = pickupStoreInfo["additionalInfo"]
        init.pickupStoreInfo_address = pickupStoreInfo["address"]
        init.pickupStoreInfo_dockId = pickupStoreInfo["dockId"]
        init.pickupStoreInfo_friendlyName = pickupStoreInfo["friendlyName"]
        init.pickupStoreInfo_isPickupStore = pickupStoreInfo["isPickupStore"]
    except:
        print("No hay datos pickupStoreInfo")
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
        print("No hay datos selectedAddresses")
    try:
        init.transactions_isActive = transactions["isActive"]
        init.transactions_transactionId = transactions["transactionId"]
        init.transactions_merchantName = transactions["merchantName"]
    except:
        print("No hay datos transactions")
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
    except:
        print("No hay datos payments")
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
        print("No hay datos billingAddress")
    try:
        init.seller_id = sellers["id"]
        init.seller_name = sellers["name"]
        init.seller_logo = sellers["logo"]
    except:
        print("No hay datos seller")
    try:
        init.changesAttachment_id = changesAttachment["id"]
    except:
        print("No hay datos changesAttachment")
    try:
        init.storePreferencesData_countryCode = storePreferencesData["countryCode"]
        init.storePreferencesData_currencyCode = storePreferencesData["currencyCode"]
        init.storePreferencesData_currencyLocale = storePreferencesData["currencyLocale"]
        init.storePreferencesData_currencySymbol = storePreferencesData["currencySymbol"]
        init.storePreferencesData_timeZone = storePreferencesData["timeZone"]
    except:
        print("No hay datos storePreferencesData")
    try:
        init.CurrencyDecimalDigits = currencyFormatInfo["CurrencyDecimalDigits"]
        init.CurrencyDecimalSeparator = currencyFormatInfo["CurrencyDecimalSeparator"]
        init.CurrencyGroupSeparator = currencyFormatInfo["CurrencyGroupSeparator"]
        init.CurrencyGroupSize = currencyFormatInfo["CurrencyGroupSize"]
        init.StartsWithCurrencySymbol = currencyFormatInfo["StartsWithCurrencySymbol"]
    except:
        print("No hay datos currencyFormatInfo")
    try:
        init.baseURL = marketplace["baseURL"]
        init.isCertified = marketplace["isCertified"]
        init.name = marketplace["name"]
    except:
        print("No hay datos marketplace")
    
    df1 = pd.DataFrame({
        'orderId': init.orderId,
        'emailTracked': init.emailTracked,
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
        'followUpEmail': init.followUpEmail,
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
        'total_id_items': init.total_id_items,
        'total_name_items': init.total_name_items,
        'total_value_items': init.total_value_items,
        'total_id_discounts': init.total_id_discounts,
        'total_name_discounts': init.total_name_discounts,
        'total_value_discounts': init.total_value_discounts,
        'total_id_shipping': init.total_id_shipping,
        'total_name_shipping': init.total_name_shipping,
        'total_value_shipping': init.total_value_shipping,
        'total_id_tax': init.total_id_tax,
        'total_name_tax': init.total_name_tax,
        'total_value_tax': init.total_value_tax,
        'total_id_change': init.total_id_change,
        'total_name_change': init.total_name_change,
        'total_value_change': init.total_value_change,
        'uniqueId': init.items_uniqueId,
        'items_id': init.items_id,
        'productId': init.items_productId,
        'ean': init.items_ean,
        'lockId': init.items_lockId,
        'quantity': init.item_quantity,
        'seller': init.item_seller,
        'item_name': init.item_name,
        'refId': init.item_refId,
        'item_price': init.item_price,
        'item_listPrice': init.item_listPrice,
        'item_manualPrice': init.item_manualPrice,
        'item_imageUrl': init.item_imageUrl,
        'item_detailUrl': init.item_detailUrl,
        'item_sellerSku': init.item_sellerSku,
        'item_priceValidUntil': init.item_priceValidUntil,
        'item_commission': init.item_commission,
        'item_tax': init.item_tax,
        'item_preSaleDate': init.item_preSaleDate,
        'item_measurementUnit': init.item_measurementUnit,
        'item_unitMultiplier': init.item_unitMultiplier,
        'item_sellingPrice': init.item_sellingPrice,
        'item_isGift': init.item_isGift,
        'item_shippingPrice': init.item_shippingPrice,
        'item_rewardValue': init.item_rewardValue,
        'item_freightCommission': init.item_freightCommission,
        'item_taxCode': init.item_taxCode,
        'item_parentItemIndex': init.item_parentItemIndex,
        'item_parentAssemblyBinding': init.item_parentAssemblyBinding,
        'item_itemAttachment_name': init.item_itemAttachment_name,
        'brandName': init.brandName,
        'brandId': init.brandId,
        'categoriesIds': init.categoriesIds,
        'productClusterId': init.productClusterId,
        'commercialConditionId': init.commercialConditionId,
        'offeringInfo': init.offeringInfo,
        'offeringType': init.offeringType,
        'offeringTypeId': init.offeringTypeId,
        'cubicweight': init.cubicweight,
        'height': init.height,
        'length': init.length,
        'weight': init.weight,
        'width': init.width,
        'calculatedSellingPrice': init.calculatedSellingPrice,
        'total': init.total,
        'client_id': init.client_id,
        'client_email': client_email_01,
        'client_firstName': init.client_firstName,
        'client_lastName': init.client_lastName,
        'client_documentType': init.client_documentType,
        'client_document': init.client_document,
        'client_phone': init.client_phone,
        'client_corporateName': init.client_corporateName,
        'client_tradeName': init.client_tradeName,
        'client_corporateDocument': init.client_corporateDocument,
        'client_stateInscription': init.client_stateInscription,
        'client_corporatePhone': init.client_corporatePhone,
        'client_isCorporate': init.client_isCorporate,
        'client_userProfileId': init.client_userProfileId,
        'client_customerClass': init.client_customerClass,
        'id_ratesAndBenefits': init.id_ratesAndBenefits,
        'shippingData_id': init.shippingData_id,
        'addressType': init.addressType,
        'receiverName': init.receiverName,
        'addressId': init.addressId,
        'postalCode': init.postalCode,
        'city': init.city,
        'state': init.state,
        'country': init.country,
        'street': init.street,
        'number': init.number,
        'neighborhood': init.neighborhood,
        'complement': init.complement,
        'reference': init.reference,
        'trackingHints': init.trackingHints,
        'deliveryChannel': init.deliveryChannel,
        'addressId': init.addressId,
        'polygonName': init.polygonName,
        'itemIndex': init.itemIndex,
        'selectedSla': init.selectedSla,
        'lockTTL': init.lockTTL,
        'price': init.price,
        'listPrice': init.listPrice,
        'sellingPrice': init.sellingPrice,
        'deliveryWindow': init.deliveryWindow,
        'deliveryCompany': init.deliveryCompany,
        'shippingEstimate': init.shippingEstimate,
        'shippingEstimateDate': init.shippingEstimateDate,
        'slas_id': init.slas_id,
        'slas_name': init.slas_name,
        'slas_shippingEstimate': init.slas_shippingEstimate,
        'slas_deliveryWindow': init.slas_deliveryWindow,
        'slas_price': init.slas_price,
        'slas_deliveryChannel': init.slas_deliveryChannel,
        'slas_polygonName': init.slas_polygonName,
        'slas_pickupStoreInfo_additionalInfo': init.slas_pickupStoreInfo_additionalInfo,
        'slas_pickupStoreInfo_address': init.slas_pickupStoreInfo_address,
        'slas_pickupStoreInfo_dockId': init.slas_pickupStoreInfo_dockId,
        'slas_pickupStoreInfo_friendlyName': init.slas_pickupStoreInfo_friendlyName,
        'slas_pickupStoreInfo_isPickupStore': init.slas_pickupStoreInfo_isPickupStore,
        'slas_id_01': init.slas_id_01,
        'slas_name_01': init.slas_name_01,
        'slas_shippingEstimate_01': init.slas_shippingEstimate_01,
        'slas_deliveryWindow_01': init.slas_deliveryWindow_01,
        'slas_price_01': init.slas_price_01,
        'slas_deliveryChannel_01': init.slas_deliveryChannel_01,
        'slas_polygonName_01': init.slas_polygonName_01,
        'slas_pickupStoreInfo_additionalInfo_01': init.slas_pickupStoreInfo_additionalInfo_01,
        'slas_pickupStoreInfo_address_01': init.slas_pickupStoreInfo_address_01,
        'slas_pickupStoreInfo_dockId_01': init.slas_pickupStoreInfo_dockId_01,
        'slas_pickupStoreInfo_friendlyName_01': init.slas_pickupStoreInfo_friendlyName_01,
        'slas_pickupStoreInfo_isPickupStore_01': init.slas_pickupStoreInfo_isPickupStore_01,
        'slas_id_02': init.slas_id_02,
        'slas_name_02': init.slas_name_02,
        'slas_shippingEstimate_02': init.slas_shippingEstimate_02,
        'slas_deliveryWindow_02': init.slas_deliveryWindow_02,
        'slas_price_02': init.slas_price_02,
        'slas_deliveryChannel_02': init.slas_deliveryChannel_02,
        'slas_polygonName_02': init.slas_polygonName_02,
        'slas_pickupStoreInfo_additionalInfo_02': init.slas_pickupStoreInfo_additionalInfo_02,
        'slas_pickupStoreInfo_address_02': init.slas_pickupStoreInfo_address_02,
        'slas_pickupStoreInfo_dockId_02': init.slas_pickupStoreInfo_dockId_02,
        'slas_pickupStoreInfo_friendlyName_02': init.slas_pickupStoreInfo_friendlyName_02,
        'slas_pickupStoreInfo_isPickupStore_02': init.slas_pickupStoreInfo_isPickupStore_02,
        'slas_id_03': init.slas_id_03,
        'slas_name_03': init.slas_name_03,
        'slas_shippingEstimate_03': init.slas_shippingEstimate_03,
        'slas_deliveryWindow_03': init.slas_deliveryWindow_03,
        'slas_price_03': init.slas_price_03,
        'slas_deliveryChannel_03': init.slas_deliveryChannel_03,
        'slas_polygonName_03': init.slas_polygonName_03,
        'slas_pickupStoreInfo_additionalInfo_03': init.slas_pickupStoreInfo_additionalInfo_03,
        'slas_pickupStoreInfo_address_03': init.slas_pickupStoreInfo_address_03,
        'slas_pickupStoreInfo_dockId_03': init.slas_pickupStoreInfo_dockId_03,
        'slas_pickupStoreInfo_friendlyName_03': init.slas_pickupStoreInfo_friendlyName_03,
        'slas_pickupStoreInfo_isPickupStore_03': init.slas_pickupStoreInfo_isPickupStore_03,
        'courierId': init.courierId,
        'courierName': init.courierName,
        'dockId': init.dockId,
        'quantity': init.quantity,
        'warehouseId': init.warehouseId,
        'pickupStoreInfo_additionalInfo': init.pickupStoreInfo_additionalInfo,
        'pickupStoreInfo_address': init.pickupStoreInfo_address,
        'pickupStoreInfo_dockId': init.pickupStoreInfo_dockId,
        'pickupStoreInfo_friendlyName': init.pickupStoreInfo_friendlyName,
        'pickupStoreInfo_isPickupStore': init.pickupStoreInfo_isPickupStore,
        'selectedAddresses_addressId': init.selectedAddresses_addressId,
        'selectedAddresses_addressType': init.selectedAddresses_addressType,
        'selectedAddresses_receiverName': init.selectedAddresses_receiverName,
        'selectedAddresses_street': init.selectedAddresses_street,
        'selectedAddresses_number': init.selectedAddresses_number,
        'selectedAddresses_complement': init.selectedAddresses_complement,
        'selectedAddresses_neighborhood': init.selectedAddresses_neighborhood,
        'selectedAddresses_postalCode': init.selectedAddresses_postalCode,
        'selectedAddresses_city': init.selectedAddresses_city,
        'selectedAddresses_state': init.selectedAddresses_state,
        'selectedAddresses_country': init.selectedAddresses_country,
        'selectedAddresses_reference': init.selectedAddresses_reference,
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
        'invoicedDate': init.invoicedDate}, index=[0])
    init.df = init.df.append(df1)
    print("Registro: "+str(reg))
        

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT orderId FROM `shopstar-datalake.landing_zone.shopstar_vtex_list_order`')
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
			'CREATE OR REPLACE TABLE `shopstar-datalake.landing_zone.shopstar_vtex_order` AS SELECT DISTINCT * FROM `shopstar-datalake.landing_zone.shopstar_vtex_order`')
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
        dataset_id = 'landing_zone'
        table_id = 'shopstar_vtex_order'
        
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