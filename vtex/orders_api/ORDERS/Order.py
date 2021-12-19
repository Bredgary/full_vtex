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
    item_price_definition = None
    item_serialNumbers = None
    
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
    payments_cardNumber = None
    payments_cvv2 = None
    payments_expireMonth = None
    payments_expireYear = None
    payments_giftCardProvider = None
    payments_giftCardAsDiscount = None
    payments_koinUrl = None
    payments_accountId = None
    payments_parentAccountId = None
    payments_bankIssuedInvoiceIdentificationNumber = None
    payments_bankIssuedInvoiceIdentificationNumberFormatted = None
    payments_ankIssuedInvoiceBarCodeNumber = None
    payments_bankIssuedInvoiceBarCodeType = None
    
    payments_Tid = None
    payments_ReturnCode = None
    payments_Message = None
    payments_authId = None
    payments_acquirer = None

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
    
    '''
    itemMetadata
    '''
    itemMetadata_Id = None
    itemMetadata_Seller = None
    itemMetadata_Name = None
    itemMetadata_SkuName = None
    itemMetadata_ProductId = None
    itemMetadata_RefId = None
    itemMetadata_Ean = None
    itemMetadata_ImageUrl = None
    itemMetadata_DetailUrl = None
    
    subscriptionData = None
    taxData = None
    checkedInPickupPointId = None
    cancellationData = None
    
    '''
    packageAttachment
    '''
    courier = None
    invoiceNumber = None
    invoiceValue = None
    invoiceUrl = None
    issuanceDate = None
    trackingNumber = None
    invoiceKey = None
    trackingUrl = None
    embeddedInvoice = None
    type = None
    courierStatus = None
    cfop = None
    restitutions = None
    volumes = None
    EnableInferItems = None
    
    '''
    invoice data
    '''
    invoice_address = None
    userPaymentInfo = None
    
    '''
    transacctions
    '''
    
    isActive = None
    transactionId = None
    merchantName = None
    
    '''
    cancellation
    '''
    RequestedByUser = None
    RequestedBySystem = None
    RequestedBySellerNotification = None
    RequestedByPaymentNotification = None
    Reason = None
    CancellationDate = None
    
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
    try:
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
        INIT DIMENSION  packageAttachment
        '''    
        try:
            packageAttachment = Fjson["packageAttachment"]
            packages = packageAttachment["packages"]
        except:
            print("packageAttachment. No tiene datos")    
        try:
            itemMetadata = Fjson["itemMetadata"]
            ItemMetadata = itemMetadata["Items"]
        except:
            print("ItemMetadata. No tiene datos")
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
            transactions = paymentData["transactions"]
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
            init.item_price_definition = Items["priceDefinition"]
            init.item_serialNumbers = Items["serialNumbers"]
        except:
            print("No hay datos dim Items")
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
            print("No hay datos payments")
        try:
            connectorResponses = payments["connectorResponses"]
            init.payments_ReturnCode = connectorResponses["ReturnCode"]
            init.payments_Message = connectorResponses["Message"]
            init.payments_authId = connectorResponses["authId"]
            init.payments_acquirer = connectorResponses["acquirer"]
        except:
            print("No hay datos connectorResponses")
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
            init.changesAttachment_id = Fjson["changesAttachment"]
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
        
        try:
            followUpEmail = decrypt_email(str(init.followUpEmail))
        except:
            followUpEmail = None
            print("nulo")
        
        try:
            client_email = decrypt_email(str(init.client_email))
        except:
            client_email = None
            print("nulo")
        
        try:
            emailTracked = decrypt_email(str(init.emailTracked))
        except:
            emailTracked = None
            print("nulo")
        
        
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
            print("vacio")
        
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
            print("vacio")
            
    
        try:
            dim_invoiceData = Fjson["invoiceData"]
            init.invoice_address = dim_invoiceData["address"]
            init.userPaymentInfo = dim_invoiceData["userPaymentInfo"]
        except:
            print("vacio")
        
        try:    
            init.isActive = transactions["isActive"]
            init.transactionId = transactions["transactionId"]
            init.merchantName = transactions["merchantName"]
        except:
            print("vacio")
            
        try:
            init.cancellationData = Fjson["cancellationData"]
            init.RequestedByUser = cancellationData["RequestedByUser"]
            init.RequestedBySystem = cancellationData["RequestedBySystem"]
            init.RequestedBySellerNotification = cancellationData["RequestedBySellerNotification"]
            init.RequestedByPaymentNotification = cancellationData["RequestedByPaymentNotification"]
            init.Reason = cancellationData["Reason"]
            init.CancellationDate = cancellationData["CancellationDate"]
        except:
            print("cancellationData")
    
        df1 = pd.DataFrame({
            'orderId': str(init.orderId),
            'emailTracked': str(emailTracked),
            'approvedBy': str(init.approvedBy),
            'cancelledBy': str(init.cancelledBy),
            'cancelReason': str(init.cancelReason),
            'sequence': str(init.sequence),
            'marketplaceOrderId': str(init.marketplaceOrderId),
            'marketplaceServicesEndpoint': str(init.marketplaceServicesEndpoint),
            'sellerOrderId': str(init.sellerOrderId),
            'origin': str(init.origin),
            'affiliateId': str(init.affiliateId),
            'salesChannel': str(init.salesChannel),
            'merchantName': str(init.merchantName),
            'status': str(init.status),
            'statusDescription': str(init.statusDescription),
            'value': str(init.value),
            'creationDate': str(init.creationDate),
            'lastChange': str(init.lastChange),
            'orderGroup': str(init.orderGroup),
            'giftRegistryData': str(init.giftRegistryData),
            'marketingData': str(init.marketingData),
            'callCenterOperatorData': str(init.callCenterOperatorData),
            'followUpEmail': str(followUpEmail),
            'lastMessage': str(init.lastMessage),
            'hostname': str(init.hostname),
            'invoiceData': str(init.invoiceData),
            'openTextField': str(init.openTextField),
            'roundingError': str(init.roundingError),
            'orderFormId': str(init.orderFormId),
            'commercialConditionData': str(init.commercialConditionData),
            'isCompleted': str(init.isCompleted),
            'customData': str(init.customData),
            'allowCancellation': str(init.allowCancellation),
            'allowEdition': str(init.allowEdition),
            'isCheckedIn': str(init.isCheckedIn),
            'authorizedDate': str(init.authorizedDate),
            'DIM_TOTAL_id_items': str(init.total_id_items),
            'DIM_TOTAL_name_items': str(init.total_name_items),
            'DIM_TOTAL_value_items': str(init.total_value_items),
            'DIM_TOTAL_id_discounts': str(init.total_id_discounts),
            'DIM_TOTAL_name_discounts': str(init.total_name_discounts),
            'DIM_TOTAL_value_discounts': str(init.total_value_discounts),
            'DIM_TOTAL_id_shipping': str(init.total_id_shipping),
            'DIM_TOTAL_name_shipping': str(init.total_name_shipping),
            'DIM_TOTAL_value_shipping': str(init.total_value_shipping),
            'DIM_TOTAL_id_tax': str(init.total_id_tax),
            'DIM_TOTAL_name_tax': str(init.total_name_tax),
            'DIM_TOTAL_value_tax': str(init.total_value_tax),
            'DIM_TOTAL_id_change': str(init.total_id_change),
            'DIM_TOTAL_name_change': str(init.total_name_change),
            'DIM_TOTAL_value_change': str(init.total_value_change),
            'DIM_ITEMS_uniqueId': str(init.items_uniqueId),
            'DIM_ITEMS_items_id': str(init.items_id),
            'DIM_ITEMS_productId': str(init.items_productId),
            'DIM_ITEMS_ean': str(init.items_ean),
            'DIM_ITEMS_lockId': str(init.items_lockId),
            'DIM_ITEMS_quantity': str(init.item_quantity),
            'DIM_ITEMS_seller': str(init.item_seller),
            'DIM_ITEMS_name': str(init.item_name),
            'DIM_ITEMS_refId': str(init.item_refId),
            'DIM_ITEMS_price': str(init.item_price),
            'DIM_ITEMS_listPrice': str(init.item_listPrice),
            'DIM_ITEMS_manualPrice': str(init.item_manualPrice),
            'DIM_ITEMS_imageUrl': str(init.item_imageUrl),
            'DIM_ITEMS_detailUrl': str(init.item_detailUrl),
            'DIM_ITEMS_sellerSku': str(init.item_sellerSku),
            'DIM_ITEMS_priceValidUntil': str(init.item_priceValidUntil),
            'DIM_ITEMS_commission': str(init.item_commission),
            'DIM_ITEMS_tax': str(init.item_tax),
            'DIM_ITEM_preSaleDate': str(init.item_preSaleDate),
            'DIM_ITEM_measurementUnit': str(init.item_measurementUnit),
            'DIM_ITEM_unitMultiplier': str(init.item_unitMultiplier),
            'DIM_ITEM_sellingPrice': str(init.item_sellingPrice),
            'DIM_ITEM_isGift': str(init.item_isGift),
            'DIM_ITEM_shippingPrice': str(init.item_shippingPrice),
            'DIM_ITEM_rewardValue': str(init.item_rewardValue),
            'DIM_ITEM_freightCommission': str(init.item_freightCommission),
            'DIM_ITEM_priceDefinition': str(init.item_price_definition),
            'DIM_ITEM_taxCode': str(init.item_taxCode),
            'DIM_ITEM_parentItemIndex': str(init.item_parentItemIndex),
            'DIM_ITEM_parentAssemblyBinding': str(init.item_parentAssemblyBinding),
            'DIM_ITEM_itemAttachment_name': str(init.item_itemAttachment_name),
            'DIM_ITEM_AInfo_brandName': str(init.brandName),
            'DIM_ITEM_AInfo_brandId': str(init.brandId),
            'DIM_ITEM_AInfo_categoriesIds': str(init.categoriesIds),
            'DIM_ITEM_AInfo_productClusterId': str(init.productClusterId),
            'DIM_ITEM_AInfo_commercialConditionId': str(init.commercialConditionId),
            'DIM_ITEM_AInfo_offeringInfo': str(init.offeringInfo),
            'DIM_ITEM_AInfo_offeringType': str(init.offeringType),
            'DIM_ITEM_AInfo_offeringTypeId': str(init.offeringTypeId),
            'DIM_ITEM_AInfo_cubicweight': str(init.cubicweight),
            'DIM_ITEM_AInfo_dim_height': str(init.height),
            'DIM_ITEM_AInfo_dim_length': str(init.length),
            'DIM_ITEM_AInfo_dim_weight': str(init.weight),
            'DIM_ITEM_AInfo_dim_width': str(init.width),
            'DIM_ITEM_calculatedSellingPrice': str(init.calculatedSellingPrice),
            'DIM_ITEM_priceDefinition_total': str(init.total),
            'DIM_CLIENT': str(init.client_id),
            'DIM_CLIENT_email': str(client_email),
            'DIM_CLIENT_firstName': str(init.client_firstName),
            'DIM_CLIENT_lastName': str(init.client_lastName),
            'DIM_CLIENT_documentType': str(init.client_documentType),
            'DIM_CLIENT_document': str(init.client_document),
            'DIM_CLIENT_phone': str(init.client_phone),
            'DIM_CLIENT_corporateName': str(init.client_corporateName),
            'DIM_CLIENT_tradeName': str(init.client_tradeName),
            'DIM_CLIENT_corporateDocument': str(init.client_corporateDocument),
            'DIM_CLIENT_stateInscription': str(init.client_stateInscription),
            'DIM_CLIENT_corporatePhone': str(init.client_corporatePhone),
            'DIM_CLIENT_isCorporate': str(init.client_isCorporate),
            'DIM_CLIENT_userProfileId': str(init.client_userProfileId),
            'DIM_CLIENT_customerClass': str(init.client_customerClass),
            'id_ratesAndBenefits': str(init.id_ratesAndBenefits),
            'DIM_SHIPPING_DATA_shippingData_id': str(init.shippingData_id),
            'DIM_SHIPPING_DATA_addressType': str(init.addressType),
            'DIM_SHIPPING_DATA_receiverName': str(init.receiverName),
            'DIM_SHIPPING_DATA_addressId': str(init.addressId),
            'DIM_SHIPPING_DATA_postalCode': str(init.postalCode),
            'DIM_SHIPPING_DATA_city': str(init.city),
            'DIM_SHIPPING_DATA_state': str(init.state),
            'DIM_SHIPPING_DATA_country': str(init.country),
            'DIM_SHIPPING_DATA_street': str(init.street),
            'DIM_SHIPPING_DATA_number': str(init.number),
            'DIM_SHIPPING_DATA_neighborhood': str(init.neighborhood),
            'DIM_SHIPPING_DATA_complement': str(init.complement),
            'DIM_SHIPPING_DATA_reference': str(init.reference),
            'DIM_SHIPPING_DATA_trackingHints': str(init.trackingHints),
            'DIM_SHIPPING_DATA_deliveryChannel': str(init.deliveryChannel),
            'DIM_SHIPPING_DATA_addressId': str(init.addressId),
            'DIM_SHIPPING_DATA_polygonName': str(init.polygonName),
            'DIM_SHIPPING_DATA_itemIndex': str(init.itemIndex),
            'DIM_SHIPPING_DATA_selectedSla': str(init.selectedSla),
            'DIM_SHIPPING_DATA_lockTTL': str(init.lockTTL),
            'DIM_SHIPPING_DATA_price': str(init.price),
            'DIM_SHIPPING_DATA_listPrice': str(init.listPrice),
            'DIM_SHIPPING_DATA_sellingPrice': str(init.sellingPrice),
            'DIM_SHIPPING_DATA_deliveryWindow': str(init.deliveryWindow),
            'DIM_SHIPPING_DATA_deliveryCompany': str(init.deliveryCompany),
            'DIM_SHIPPING_DATA_shippingEstimate': str(init.shippingEstimate),
            'DIM_SHIPPING_DATA_shippingEstimateDate': str(init.shippingEstimateDate),
            'DIM_SHIPPING_DATA_slas_id': str(init.slas_id),
            'DIM_SHIPPING_DATA_slas_name': str(init.slas_name),
            'DIM_SHIPPING_DATA_slas_shippingEstimate': str(init.slas_shippingEstimate),
            'DIM_SHIPPING_DATA_slas_deliveryWindow': str(init.slas_deliveryWindow),
            'DIM_SHIPPING_DATA_slas_price': str(init.slas_price),
            'DIM_SHIPPING_DATA_slas_deliveryChannel': str(init.slas_deliveryChannel),
            'DIM_SHIPPING_DATA_slas_polygonName': str(init.slas_polygonName),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo': str(init.slas_pickupStoreInfo_additionalInfo),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_address': str(init.slas_pickupStoreInfo_address),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId': str(init.slas_pickupStoreInfo_dockId),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName': str(init.slas_pickupStoreInfo_friendlyName),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore': str(init.slas_pickupStoreInfo_isPickupStore),
            'DIM_SHIPPING_DATA_slas_id_01': str(init.slas_id_01),
            'DIM_SHIPPING_DATA_slas_name_01': str(init.slas_name_01),
            'DIM_SHIPPING_DATA_slas_shippingEstimate_01': str(init.slas_shippingEstimate_01),
            'DIM_SHIPPING_DATA_slas_deliveryWindow_01': str(init.slas_deliveryWindow_01),
            'DIM_SHIPPING_DATA_slas_price_01': str(init.slas_price_01),
            'DIM_SHIPPING_DATA_slas_deliveryChannel_01': str(init.slas_deliveryChannel_01),
            'DIM_SHIPPING_DATA_slas_polygonName_01': str(init.slas_polygonName_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_01': str(init.slas_pickupStoreInfo_additionalInfo_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_address_01': str(init.slas_pickupStoreInfo_address_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_01': str(init.slas_pickupStoreInfo_dockId_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_01': str(init.slas_pickupStoreInfo_friendlyName_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_01': str(init.slas_pickupStoreInfo_isPickupStore_01),
            'DIM_SHIPPING_DATA_slas_id_02': str(init.slas_id_02),
            'DIM_SHIPPING_DATA_slas_name_02': str(init.slas_name_02),
            'DIM_SHIPPING_DATA_slas_shippingEstimate_02': str(init.slas_shippingEstimate_02),
            'DIM_SHIPPING_DATA_slas_deliveryWindow_02': str(init.slas_deliveryWindow_02),
            'DIM_SHIPPING_DATA_slas_price_02': str(init.slas_price_02),
            'DIM_SHIPPING_DATA_slas_deliveryChannel_02': str(init.slas_deliveryChannel_02),
            'DIM_SHIPPING_DATA_slas_polygonName_02': str(init.slas_polygonName_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_02': str(init.slas_pickupStoreInfo_additionalInfo_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_address_02': str(init.slas_pickupStoreInfo_address_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_02': str(init.slas_pickupStoreInfo_dockId_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_02': str(init.slas_pickupStoreInfo_friendlyName_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_02': str(init.slas_pickupStoreInfo_isPickupStore_02),
            'DIM_SHIPPING_DATA_slas_id_03': str(init.slas_id_03),
            'DIM_SHIPPING_DATA_slas_name_03': str(init.slas_name_03),
            'DIM_SHIPPING_DATA_slas_shippingEstimate_03': str(init.slas_shippingEstimate_03),
            'DIM_SHIPPING_DATA_slas_deliveryWindow_03': str(init.slas_deliveryWindow_03),
            'DIM_SHIPPING_DATA_slas_price_03': str(init.slas_price_03),
            'DIM_SHIPPING_DATA_slas_deliveryChannel_03': str(init.slas_deliveryChannel_03),
            'DIM_SHIPPING_DATA_slas_polygonName_03': str(init.slas_polygonName_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_03': str(init.slas_pickupStoreInfo_additionalInfo_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_address_03': str(init.slas_pickupStoreInfo_address_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_03': str(init.slas_pickupStoreInfo_dockId_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_03': str(init.slas_pickupStoreInfo_friendlyName_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_03': str(init.slas_pickupStoreInfo_isPickupStore_03),
            'DIM_SHIPPING_DATA_courierId_delivery': str(init.courierId),
            'DIM_SHIPPING_DATA_courierName_delivery': str(init.courierName),
            'DIM_SHIPPING_DATA_dockId_delivery': str(init.dockId),
            'DIM_SHIPPING_DATA_quantity_delivery': str(init.quantity),
            'DIM_SHIPPING_DATA_warehouseId': str(init.warehouseId),
            'DIM_SHIPPING_DATA_pickupStoreInfo_additionalInfo': str(init.pickupStoreInfo_additionalInfo),
            'DIM_SHIPPING_DATA_pickupStoreInfo_address': str(init.pickupStoreInfo_address),
            'DIM_SHIPPING_DATA_pickupStoreInfo_dockId': str(init.pickupStoreInfo_dockId),
            'DIM_SHIPPING_DATA_pickupStoreInfo_friendlyName': str(init.pickupStoreInfo_friendlyName),
            'DIM_SHIPPING_DATA_pickupStoreInfo_isPickupStore': str(init.pickupStoreInfo_isPickupStore),
            'DIM_SHIPPING_DATA_selectedAddresses_addressId': str(init.selectedAddresses_addressId),
            'DIM_SHIPPING_DATA_selectedAddresses_addressType': str(init.selectedAddresses_addressType),
            'DIM_SHIPPING_DATA_selectedAddresses_receiverName': str(init.selectedAddresses_receiverName),
            'DIM_SHIPPING_DATA_selectedAddresses_street': str(init.selectedAddresses_street),
            'DIM_SHIPPING_DATA_selectedAddresses_number': str(init.selectedAddresses_number),
            'DIM_SHIPPING_DATA_selectedAddresses_complement': str(init.selectedAddresses_complement),
            'DIM_SHIPPING_DATA_selectedAddresses_neighborhood': str(init.selectedAddresses_neighborhood),
            'DIM_SHIPPING_DATA_selectedAddresses_postalCode': str(init.selectedAddresses_postalCode),
            'DIM_SHIPPING_DATA_selectedAddresses_city': str(init.selectedAddresses_city),
            'DIM_SHIPPING_DATA_selectedAddresses_state': str(init.selectedAddresses_state),
            'DIM_SHIPPING_DATA_selectedAddresses_country': str(init.selectedAddresses_country),
            'DIM_SHIPPING_DATA_selectedAddresses_reference': str(init.selectedAddresses_reference),
            'transactions_isActive': str(init.transactions_isActive),
            'transactions_transactionId': str(init.transactions_transactionId),
            'transactions_merchantName': str(init.transactions_merchantName),
            'payments_id': str(init.payments_id),
            'payments_paymentSystem': str(init.payments_paymentSystem),
            'payments_paymentSystemName': str(init.payments_paymentSystemName),
            'payments_value': str(init.payments_value),
            'payments_installments': str(init.payments_installments),
            'payments_referenceValue': str(init.payments_referenceValue),
            'payments_cardHolder': str(init.payments_cardHolder),
            'payments_firstDigits': str(init.payments_firstDigits),
            'payments_lastDigits': str(init.payments_lastDigits),
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
            'payments_ReturnCode': str(init.payments_ReturnCode),
            'payments_Message': str(init.payments_Message),
            'payments_authId': str(init.payments_authId),
            'payments_acquirer': str(init.payments_acquirer),
            'billingAddress_postalCode': str(init.billingAddress_postalCode),
            'billingAddress_city': str(init.billingAddress_city),
            'billingAddress_state': str(init.billingAddress_state),
            'billingAddress_country': str(init.billingAddress_country),
            'billingAddress_street': str(init.billingAddress_street),
            'billingAddress_number': str(init.billingAddress_number),
            'billingAddress_neighborhood': str(init.billingAddress_neighborhood),
            'billingAddress_complement': str(init.billingAddress_complement),
            'billingAddress_reference': str(init.billingAddress_reference),
            'seller_id': str(init.seller_id),
            'seller_name': str(init.seller_name),
            'seller_logo': str(init.seller_logo),
            'changesAttachment_id': str(init.changesAttachment_id),
            'storePreferencesData_countryCode': str(init.storePreferencesData_countryCode),
            'storePreferencesData_currencyCode': str(init.storePreferencesData_currencyCode),
            'storePreferencesData_currencyLocale': str(init.storePreferencesData_currencyLocale),
            'storePreferencesData_currencySymbol': str(init.storePreferencesData_currencySymbol),
            'storePreferencesData_timeZone': str(init.storePreferencesData_timeZone),
            'CurrencyDecimalDigits': str(init.CurrencyDecimalDigits),
            'CurrencyDecimalSeparator': str(init.CurrencyDecimalSeparator),
            'CurrencyGroupSeparator': str(init.CurrencyGroupSeparator),
            'CurrencyGroupSize': str(init.CurrencyGroupSize),
            'StartsWithCurrencySymbol': str(init.StartsWithCurrencySymbol),
            'marketplace_baseURL': str(init.baseURL),
            'marketplace_isCertified': str(init.isCertified),
            'marketplace_name': str(init.name),
            'itemMetadata_Id': str(init.itemMetadata_Id),
            'itemMetadata_Seller': str(init.itemMetadata_Seller),
            'itemMetadata_Name': str(init.itemMetadata_Name),
            'itemMetadata_SkuName': str(init.itemMetadata_SkuName),
            'itemMetadata_ProductId': str(init.itemMetadata_ProductId),
            'itemMetadata_RefId': str(init.itemMetadata_RefId),
            'itemMetadata_Ean': str(init.itemMetadata_Ean),
            'itemMetadata_ImageUrl': str(init.itemMetadata_ImageUrl),
            'itemMetadata_DetailUrl': str(init.itemMetadata_DetailUrl),
            'subscriptionData': str(init.subscriptionData),
            'taxData': str(init.taxData),
            'cancellationData': str(init.cancellationData),
            'courier': str(init.courier),
            'invoiceNumber': str(init.invoiceNumber),
            'invoiceValue': str(init.invoiceValue),
            'invoiceUrl': str(init.invoiceUrl),
            'issuanceDate': str(init.issuanceDate),
            'trackingNumber': str(init.trackingNumber),
            'invoiceKey': str(init.invoiceKey),
            'trackingUrl': str(init.trackingUrl),
            'embeddedInvoice': str(init.embeddedInvoice),
            'type': str(init.type),
            'courierStatus': str(init.courierStatus),
            'cfop': str(init.cfop),
            'restitutions': str(init.restitutions),
            'volumes': str(init.volumes),
            'EnableInferItems': str(init.EnableInferItems),
            'invoice_address': str(init.invoice_address),
            'userPaymentInfo': str(init.userPaymentInfo),
            'serialNumbers':str(init.item_serialNumbers),
            'isActive':str(init.isActive),
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
        
        print("Registro: "+str(reg))
    except:
        print("vacio")

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


def run():
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_vtex_order'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()        


def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId,registro)
        if registro == 3:
            break
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
    run()
        
    
get_params()