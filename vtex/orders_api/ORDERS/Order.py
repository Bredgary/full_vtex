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
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def dicMemberCheck(key, dicObj):
    if key in dicObj:
        return True
    else:
        return False

def decrypt_email(email):
    try:
        url = "https://conversationtracker.vtex.com.br/api/pvt/emailMapping?an=mercury&alias="+email+""
        response = requests.request("GET", url, headers=init.headers)
        formatoJ = json.loads(response.text)
        return formatoJ["email"]
    except:
        cache = 2
     
def get_order(id,reg):
    try:
        print("Registro: "+str(reg))
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        try:
            subscriptionData = Fjson["subscriptionData"]
        except:
            subscriptionData = None
        try:
            taxData = Fjson["taxData"]
        except:
            taxData = None
        try:
            checkedInPickupPointId = Fjson["checkedInPickupPointId"]
        except:
            checkedInPickupPointId = None
        try:
            cancellationData = Fjson["cancellationData"]
        except:
            cancellationData = None
        try:
            emailTracked = Fjson["emailTracked"]
        except:
            emailTracked = None
        try:
            approvedBy = Fjson["approvedBy"]
        except:
            approvedBy = None
        try:
            cancelledBy = Fjson["cancelledBy"]
        except:
            cancelledBy = None
        try:
            cancelReason = Fjson["cancelReason"]
        except:
            cancelReason = None
        try:
            orderId = Fjson["orderId"]
        except:
            orderId = None
        try:
            sequence = Fjson["sequence"]
        except:
            sequence = None
        try:
            marketplaceOrderId = Fjson["marketplaceOrderId"]
        except:
            marketplaceOrderId = None
        try:
            marketplaceServicesEndpoint = Fjson["marketplaceServicesEndpoint"]
        except:
            marketplaceServicesEndpoint = None
        try:
            sellerOrderId = Fjson["sellerOrderId"]
        except:
            sellerOrderId = None
        try:
            origin = Fjson["origin"]
        except:
            origin = None
        try:
            affiliateId = Fjson["affiliateId"]
        except:
            affiliateId = None
        try:
            salesChannel = Fjson["salesChannel"]
        except:
            salesChannel = None
        try:
            merchantName = Fjson["merchantName"]
        except:
            merchantName = None
        try:
            status = Fjson["status"]
        except:
            status = None
        try:
            statusDescription = Fjson["statusDescription"]
        except:
            statusDescription = None
        try:
            value = Fjson["value"]
        except:
            value = None
        try:
            creationDate = Fjson["creationDate"]
        except:
            creationDate = None
        try:
            lastChange = Fjson["lastChange"]
        except:
            lastChange = None
        try:
            orderGroup = Fjson["orderGroup"]
        except:
            orderGroup = None
        try:
            giftRegistryData = Fjson["giftRegistryData"]
        except:
            giftRegistryData = None
        try:
            marketingData = Fjson["marketingData"]
        except:
            marketingData = None
        try:
            callCenterOperatorData = Fjson["callCenterOperatorData"]
        except:
            callCenterOperatorData = None
        try:
            followUpEmail = Fjson["followUpEmail"]
        except:
            followUpEmail = None
        try:
            lastMessage = Fjson["lastMessage"]
        except:
            lastMessage = None
        try:
            hostname = Fjson["hostname"]
        except:
            hostname = None
        try:
            invoiceData = Fjson["invoiceData"]
        except:
            invoiceData = None
        try:
            openTextField = Fjson["openTextField"]
        except:
            openTextField = None
        try:
            roundingError = Fjson["roundingError"]
        except:
            roundingError = None
        try:
            orderFormId = Fjson["orderFormId"]
        except:
            orderFormId = None
        try:
            commercialConditionData = Fjson["commercialConditionData"]
        except:
            commercialConditionData = None
        try:
            isCompleted = Fjson["isCompleted"]
        except:
            isCompleted = None
        try:
            customData = Fjson["customData"]
        except:
            customData = None
        try:
            allowCancellation = Fjson["allowCancellation"]
        except:
            allowCancellation = None
        try:
            allowEdition = Fjson["allowEdition"]
        except:
            allowEdition = None
        try:
            isCheckedIn = Fjson["isCheckedIn"]
        except:
            isCheckedIn = None
        try:
            authorizedDate = Fjson["authorizedDate"]
        except:
            authorizedDate = None
        try:
            invoicedDate = Fjson["invoicedDate"]
        except:
            invoicedDate = None
        
        
        '''
        INIT DIMENSION  packageAttachment
        '''    
        try:
            packageAttachment = Fjson["packageAttachment"]
            packages = packageAttachment["packages"]
        except:
            cache = 2 
        try:
            itemMetadata = Fjson["itemMetadata"]
            ItemMetadata = itemMetadata["Items"]
        except:
            cache = 2
        try:
            Total = Fjson["totals"]
        except:
            cache = 2
        try:
            clientProfileData = Fjson["clientProfileData"]
        except:
            cache = 2
        try:
            marketplace = Fjson["marketplace"]
        except:
           cache = 2
        try:
            ratesAndBenefitsData = Fjson["ratesAndBenefitsData"]
        except:
            cache = 2
        try:
            storePreferencesData = Fjson["storePreferencesData"]
        except:
            cache = 2
        try:
            currencyFormatInfo = storePreferencesData["currencyFormatInfo"]
        except:
            cache = 2
        try:
            shippingData = Fjson["shippingData"]
        except:
            cache = 2
        try:
            logisticsInfo_0 = shippingData["logisticsInfo"]
        except:
            cache = 2
        try:
            selectedAddresses_ = shippingData["selectedAddresses"]
        except:
            cache = 2
        try:
            selectedAddresses = selectedAddresses_[0]
        except:
            cache = 2
        try:
            logisticsInfo = logisticsInfo_0[0]
        except:
            cache = 2
        try:
            address = shippingData["address"]
        except:
            cache = 2
        try:
            slas = logisticsInfo["slas"]
        except:
            cache = 2
        try:
            deliveryIds_ = logisticsInfo["deliveryIds"]
        except:
            cache = 2
        try:
            deliveryIds = deliveryIds_[0]
        except:
            cache = 2
        try:
            pickupStoreInfo = logisticsInfo["pickupStoreInfo"]
        except:
            cache = 2
        try:
            slas_0 = slas[0]
            pickupStoreInfo = slas_0["pickupStoreInfo"]
        except:
            cache = 2
        try:
            slas_1 = slas[1]
            pickupStoreInfo_1 = slas_1["pickupStoreInfo"]
        except:
            cache = 2
        try:
            slas_2 = slas[2]
            pickupStoreInfo_2 = slas_2["pickupStoreInfo"]
        except:
            cache = 2
        try:
            slas_3 = slas[3]
            pickupStoreInfo_3 = slas_3["pickupStoreInfo"]
        except:
            cache = 2
        try:
            items = Fjson["items"]
        except:
            cache = 2
        try:
            changesAttachment = Fjson["changesAttachment"]
        except:
            cache = 2
        try:
            paymentData = Fjson["paymentData"]
            transactions = paymentData["transactions"]
        except:
            cache = 2
        try:
            sellers_ = Fjson["sellers"]
        except:
            cache = 2
        try:
            sellers = sellers_[0]
        except:
            cache = 2
        try:
            transactions_ = paymentData["transactions"]
        except:
            cache = 2
        try:
            transactions = transactions_[0]
        except:
            cache = 2
        try:
            payments_ = transactions["payments"]
        except:
            cache = 2
        try:
            payments = payments_[0]
        except:
            cache = 2
        try:
            billingAddress = payments["billingAddress"]
        except:
            cache = 2
        try:
            Items = items[0]
        except:
            cache = 2
        try:
            itemAttachment = Items["itemAttachment"]
        except:
            cache = 2
        try:
            additionalInfo = Items["additionalInfo"]
        except:
            cache = 2
        try:
            priceDefinition = Items["priceDefinition"]
        except:
            cache = 2
        try:
            sellingPrice = Items["sellingPrice"]
        except:
            cache = 2
        try:
            dimension = additionalInfo["dimension"]
        except:
            cache = 2
        
        '''
        END DIMENSION
        '''
        
        
        try:
            if Total[0]:
                items = Total[0]
                total_id_items = items["id"]
                total_name_items = items["name"]
                total_value_items = items["value"]
        except:
            cache = 2
        try:
            if Total[1]:
                discounts = Total[1]
                total_id_discounts = discounts["id"]
                total_name_discounts = discounts["name"]
                total_value_discounts = discounts["value"]
        except:
            cache = 2
        try:
            if Total[2]:
                shipping = Total[2]
                total_id_shipping = shipping["id"]
                total_name_shipping = shipping["name"]
                total_value_shipping = shipping["value"]
        except:
            cache = 2
        try:
            if Total[3]:
                tax = Total[3]
                total_id_tax = tax["id"]
                total_name_tax = tax["name"]
                total_value_tax = tax["value"]
        except:
            cache = 2
        try:
            items_uniqueId = Items["uniqueId"]
            items_id = Items["id"]
            items_productId = Items["productId"]
            items_ean = Items["ean"]
            items_lockId = Items["lockId"]
            item_quantity = Items["quantity"]
            item_seller = Items["seller"]
            item_name = Items["name"]
            item_refId = Items["refId"]
            item_price = Items["price"]
            item_listPrice = Items["listPrice"]
            item_manualPrice = Items["manualPrice"]
            item_imageUrl = Items["imageUrl"]
            item_detailUrl = Items["detailUrl"]
            item_sellerSku = Items["sellerSku"]
            item_priceValidUntil = Items["priceValidUntil"]
            item_commission = Items["commission"]
            item_tax = Items["tax"]
            item_preSaleDate = Items["preSaleDate"]
            item_measurementUnit = Items["measurementUnit"]
            item_unitMultiplier = Items["unitMultiplier"]
            item_sellingPrice = Items["sellingPrice"]
            item_isGift = Items["isGift"]
            item_shippingPrice = Items["shippingPrice"]
            item_rewardValue = Items["rewardValue"]
            item_freightCommission = Items["freightCommission"]
            item_taxCode = Items["taxCode"]
            item_parentItemIndex = Items["parentItemIndex"]
            item_parentAssemblyBinding = Items["parentAssemblyBinding"]
            item_price_definition = Items["priceDefinition"]
            item_serialNumbers = Items["serialNumbers"]
        except:
            cache = 2
        try:
            brandName = additionalInfo["brandName"]
            brandId = additionalInfo["brandId"]
            categoriesIds = additionalInfo["categoriesIds"]
            productClusterId = additionalInfo["productClusterId"]
            commercialConditionId = additionalInfo["commercialConditionId"]
            offeringInfo = additionalInfo["offeringInfo"]
            offeringType = additionalInfo["offeringType"]
            offeringTypeId = additionalInfo["offeringTypeId"]
        except:
            cache = 2
        try:
            cubicweight = dimension["cubicweight"]
            height = dimension["height"]
            length = dimension["length"]
            weight = dimension["weight"]
            width = dimension["width"]
        except:
            cache = 2
        try:
            item_itemAttachment_name = itemAttachment["name"]
        except:
            cache = 2
        try:
            client_id = clientProfileData["id"]
            client_email = clientProfileData["email"]
            client_firstName = clientProfileData["firstName"]
            client_lastName = clientProfileData["lastName"]
            client_documentType = clientProfileData["documentType"]
            client_document = clientProfileData["document"]
            client_phone = clientProfileData["phone"]
            client_corporateName = clientProfileData["corporateName"]
            client_tradeName = clientProfileData["tradeName"]
            client_corporateDocument = clientProfileData["corporateDocument"]
            client_stateInscription = clientProfileData["stateInscription"]
            client_corporatePhone = clientProfileData["corporatePhone"]
            client_isCorporate = clientProfileData["isCorporate"]
            client_userProfileId = clientProfileData["userProfileId"]
            client_customerClass = clientProfileData["customerClass"]     
        except:
            cache = 2
        try:
            id_ratesAndBenefits = ratesAndBenefitsData["id"]
        except:
            cache = 2
        try:
            shippingData_id = shippingData["id"]
        except:
            cache = 2
        try:
            addressType = address["addressType"]
            receiverName = address["receiverName"]
            addressId = address["addressId"]
            postalCode = address["postalCode"]
            city = address["city"]
            state = address["state"]
            country = address["country"]
            street = address["street"]
            number = address["number"]
            neighborhood = address["neighborhood"]
            complement = address["complement"]
            reference = address["reference"]
        except:
            cache = 2
            
        try:
            trackingHints = logisticsInfo_0[0]
            deliveryChannel = logisticsInfo["deliveryChannel"]
            addressId = logisticsInfo["addressId"]
            polygonName = logisticsInfo["polygonName"]
            itemIndex = logisticsInfo["itemIndex"]
            selectedSla = logisticsInfo["selectedSla"]
            lockTTL = logisticsInfo["lockTTL"]
            price = logisticsInfo["price"]
            listPrice = logisticsInfo["listPrice"]
            sellingPrice = logisticsInfo["sellingPrice"]
            deliveryWindow = logisticsInfo["deliveryWindow"]
            deliveryCompany = logisticsInfo["deliveryCompany"]
            shippingEstimate = logisticsInfo["shippingEstimate"]
            shippingEstimateDate = logisticsInfo["shippingEstimateDate"]
        except:
            cache = 2
        try:
            slas_id = slas_0["id"]
            slas_name = slas_0["name"]
            slas_shippingEstimate = slas_0["shippingEstimate"]
            slas_deliveryWindow = slas_0["deliveryWindow"]
            slas_price = slas_0["price"]
            slas_deliveryChannel = slas_0["deliveryChannel"]
            slas_polygonName = slas_0["polygonName"]
        except:
            cache = 2
            
        try:
            slas_pickupStoreInfo_additionalInfo = pickupStoreInfo["additionalInfo"]
            slas_pickupStoreInfo_address = pickupStoreInfo["address"]
            slas_pickupStoreInfo_dockId = pickupStoreInfo["dockId"]
            slas_pickupStoreInfo_friendlyName = pickupStoreInfo["friendlyName"]
            slas_pickupStoreInfo_isPickupStore = pickupStoreInfo["isPickupStore"]
        except:
            cache = 2
        try:
            slas_id_01 = slas_1["id"]
            slas_name_01 = slas_1["name"]
            slas_shippingEstimate_01 = slas_1["shippingEstimate"]
            slas_deliveryWindow_01 = slas_1["deliveryWindow"]
            slas_price_01 = slas_1["price"]
            slas_deliveryChannel_01 = slas_1["deliveryChannel"]
            slas_polygonName_01 = slas_1["polygonName"]
        except:
            slas_id_01 = None
            slas_name_01 = None
            slas_shippingEstimate_01 = None
            slas_deliveryWindow_01 = None
            slas_price_01 = None
            slas_deliveryChannel_01 = None
            slas_polygonName_01 = None
        try:
            slas_pickupStoreInfo_additionalInfo_01 = pickupStoreInfo_1["additionalInfo"]
            slas_pickupStoreInfo_address_01 = pickupStoreInfo_1["address"]
            slas_pickupStoreInfo_dockId_01 = pickupStoreInfo_1["dockId"]
            slas_pickupStoreInfo_friendlyName_01 = pickupStoreInfo_1["friendlyName"]
            slas_pickupStoreInfo_isPickupStore_01 = pickupStoreInfo_1["isPickupStore"]
        except:
            slas_pickupStoreInfo_additionalInfo_01 = None
            slas_pickupStoreInfo_address_01 = None
            slas_pickupStoreInfo_dockId_01 = None
            slas_pickupStoreInfo_friendlyName_01  = None
            slas_pickupStoreInfo_isPickupStore_01 = None
        
        try:
            slas_id_02 = slas_2["id"]
            slas_name_02 = slas_2["name"]
            slas_shippingEstimate_02 = slas_2["shippingEstimate"]
            slas_deliveryWindow_02 = slas_2["deliveryWindow"]
            slas_price_02 = slas_2["price"]
            slas_deliveryChannel_02 = slas_2["deliveryChannel"]
            slas_polygonName_02 = slas_2["polygonName"]
        except:
            slas_id_02 = None
            slas_name_02 = None
            slas_shippingEstimate_02 = None
            slas_deliveryWindow_02 = None
            slas_price_02 = None
            slas_deliveryChannel_02 = None
            slas_polygonName_02 = None
        try:
            slas_pickupStoreInfo_additionalInfo_02 = pickupStoreInfo_2["additionalInfo"]
            slas_pickupStoreInfo_address_02 = pickupStoreInfo_2["address"]
            slas_pickupStoreInfo_dockId_02 = pickupStoreInfo_2["dockId"]
            slas_pickupStoreInfo_friendlyName_02 = pickupStoreInfo_2["friendlyName"]
            slas_pickupStoreInfo_isPickupStore_02 = pickupStoreInfo_2["isPickupStore"]
        except:
            slas_pickupStoreInfo_additionalInfo_02 = None
            slas_pickupStoreInfo_address_02 = None
            slas_pickupStoreInfo_dockId_02 = None
            slas_pickupStoreInfo_friendlyName_02 = None
            slas_pickupStoreInfo_isPickupStore_02 = None
            
        try:
            slas_id_03 = slas_3["id"]
            slas_name_03 = slas_3["name"]
            slas_shippingEstimate_03 = slas_3["shippingEstimate"]
            slas_deliveryWindow_03 = slas_3["deliveryWindow"]
            slas_price_03 = slas_3["price"]
            slas_deliveryChannel_03 = slas_3["deliveryChannel"]
            slas_polygonName_03 = slas_3["polygonName"]
        except:
            slas_id_03 = None
            slas_name_03 = None
            slas_shippingEstimate_03 = None
            slas_deliveryWindow_03 = None
            slas_price_03 = None
            slas_deliveryChannel_03 = None
            slas_polygonName_03 = None
        try: 
            slas_pickupStoreInfo_additionalInfo_03 = pickupStoreInfo_3["additionalInfo"]
            slas_pickupStoreInfo_address_03 = pickupStoreInfo_3["address"]
            slas_pickupStoreInfo_dockId_03 = pickupStoreInfo_3["dockId"]
            slas_pickupStoreInfo_friendlyName_03 = pickupStoreInfo_3["friendlyName"]
            slas_pickupStoreInfo_isPickupStore_03 = pickupStoreInfo_3["isPickupStore"]
        except:
            slas_pickupStoreInfo_additionalInfo_03 = None
            slas_pickupStoreInfo_address_03 = None
            slas_pickupStoreInfo_dockId_03 = None
            slas_pickupStoreInfo_friendlyName_03 = None
            slas_pickupStoreInfo_isPickupStore_03 = None
        try:
            courierId = deliveryIds["courierId"]
            courierName = deliveryIds["courierName"]
            dockId = deliveryIds["dockId"]
            quantity = deliveryIds["quantity"]
            warehouseId = deliveryIds["warehouseId"]
        except:
            courierId = None
            courierName = None
            dockId = None
            quantity = None
            warehouseId = None
        try: 
            pickupStoreInfo_additionalInfo = pickupStoreInfo["additionalInfo"]
            pickupStoreInfo_address = pickupStoreInfo["address"]
            pickupStoreInfo_dockId = pickupStoreInfo["dockId"]
            pickupStoreInfo_friendlyName = pickupStoreInfo["friendlyName"]
            pickupStoreInfo_isPickupStore = pickupStoreInfo["isPickupStore"]
        except:
            pickupStoreInfo_additionalInfo = None
            pickupStoreInfo_address = None
            pickupStoreInfo_dockId = None
            pickupStoreInfo_friendlyName = None
            pickupStoreInfo_isPickupStore = None
        try:
            selectedAddresses_addressId = selectedAddresses["addressId"]
            selectedAddresses_addressType = selectedAddresses["addressType"]
            selectedAddresses_receiverName = selectedAddresses["receiverName"]
            selectedAddresses_street = selectedAddresses["street"]
            selectedAddresses_number = selectedAddresses["number"]
            selectedAddresses_complement = selectedAddresses["complement"]
            selectedAddresses_neighborhood = selectedAddresses["neighborhood"]
            selectedAddresses_postalCode = selectedAddresses["postalCode"]
            selectedAddresses_city = selectedAddresses["city"]
            selectedAddresses_state = selectedAddresses["state"]
            selectedAddresses_country = selectedAddresses["country"]
            selectedAddresses_reference = selectedAddresses["reference"]
        except:
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
        try:
            transactions_isActive = transactions["isActive"]
            transactions_transactionId = transactions["transactionId"]
            transactions_merchantName = transactions["merchantName"]
        except:
            transactions_isActive = None
            transactions_transactionId = None
            transactions_merchantName = None
        try:
            payments_id = payments["id"]
            payments_paymentSystem = payments["paymentSystem"]
            payments_paymentSystemName = payments["paymentSystemName"]
            payments_value = payments["value"]
            payments_installments = payments["installments"]
            payments_referenceValue = payments["referenceValue"]
            payments_cardHolder = payments["cardHolder"]
            payments_firstDigits = payments["firstDigits"]
            payments_lastDigits = payments["lastDigits"]
            payments_url = payments["url"]
            payments_giftCardId = payments["giftCardId"]
            payments_giftCardName = payments["giftCardName"]
            payments_giftCardCaption = payments["giftCardCaption"]
            payments_redemptionCode = payments["redemptionCode"]
            payments_group = payments["group"]
            payments_dueDate = payments["dueDate"]
            payments_cardNumber = payments["cardNumber"]
            payments_cvv2 = payments["cvv2"]
            payments_expireMonth = payments["expireMonth"]
            payments_expireYear = payments["expireYear"]
            payments_giftCardProvider = payments["giftCardProvider"]
            payments_giftCardAsDiscount = payments["giftCardAsDiscount"]
            payments_koinUrl = payments["koinUrl"]
            payments_accountId = payments["accountId"]
            payments_parentAccountId = payments["parentAccountId"]
            payments_bankIssuedInvoiceIdentificationNumber = payments["bankIssuedInvoiceIdentificationNumber"]
            payments_bankIssuedInvoiceIdentificationNumberFormatted = payments["bankIssuedInvoiceIdentificationNumberFormatted"]
            payments_bankIssuedInvoiceBarCodeNumber = payments["bankIssuedInvoiceBarCodeNumber"]
            payments_bankIssuedInvoiceBarCodeType = payments["bankIssuedInvoiceBarCodeType"]
        except:
            cache = 2
        try:
            payments_tid = payments["tid"]
        except:
            payments_tid = None
        try:
            connectorResponses = payments["connectorResponses"]
            payments_ReturnCode = connectorResponses["ReturnCode"]
            payments_Message = connectorResponses["Message"]
            payments_authId = connectorResponses["authId"]
        except:
            connectorResponses = None
            payments_ReturnCode = None
            payments_Message = None
            payments_authId = None
        try:
            payments_acquirer = connectorResponses["acquirer"]
        except:
            payments_acquirer = None
        try:
            billingAddress_city = billingAddress["city"]
        except:
            billingAddress_city = None
        try:
            billingAddress_state = billingAddress["state"]
        except:
            billingAddress_state = None
        try:
            billingAddress_country = billingAddress["country"]
        except:
            billingAddress_country = None
        try:
            billingAddress_street = billingAddress["street"]
        except:
            billingAddress_street = None
        try:
            billingAddress_number = billingAddress["number"]
        except:
            billingAddress_number = None
        try:
            billingAddress_neighborhood = billingAddress["neighborhood"]
        except:
            billingAddress_neighborhood = None
        try:
            billingAddress_complement = billingAddress["complement"]
        except:
            billingAddress_complement = None
        try:
            billingAddress_reference = billingAddress["reference"]
        except:
            billingAddress_reference = None
        try:
            billingAddress_postalCode = billingAddress["postalCode"]
        except:
            billingAddress_postalCode = None
        try:
            seller_id = sellers["id"]
            seller_name = sellers["name"]
            seller_logo = sellers["logo"]
        except:
            cache = 2
        try:
            changesAttachment_id = Fjson["changesAttachment"]
        except:
            cache = 2
        try:
            storePreferencesData_countryCode = storePreferencesData["countryCode"]
            storePreferencesData_currencyCode = storePreferencesData["currencyCode"]
            storePreferencesData_currencyLocale = storePreferencesData["currencyLocale"]
            storePreferencesData_currencySymbol = storePreferencesData["currencySymbol"]
            storePreferencesData_timeZone = storePreferencesData["timeZone"]
        except:
            cache = 2
        try:
            CurrencyDecimalDigits = currencyFormatInfo["CurrencyDecimalDigits"]
            CurrencyDecimalSeparator = currencyFormatInfo["CurrencyDecimalSeparator"]
            CurrencyGroupSeparator = currencyFormatInfo["CurrencyGroupSeparator"]
            CurrencyGroupSize = currencyFormatInfo["CurrencyGroupSize"]
            StartsWithCurrencySymbol = currencyFormatInfo["StartsWithCurrencySymbol"]
        except:
            cache = 2
        try:
            baseURL = marketplace["baseURL"]
            isCertified = marketplace["isCertified"]
            name = marketplace["name"]
        except:
            baseURL = None
            isCertified = None
            name = None
        
        try:
            client_email = decrypt_email(str(client_email))
        except:
            client_email = None
            
        
        
        
        
        try:
            for x in ItemMetadata:
                itemMetadata_Id = x["Id"]
                itemMetadata_Seller = x["Seller"]
                itemMetadata_Name = x["Name"]
                itemMetadata_SkuName = x["SkuName"]
                itemMetadata_ProductId = x["ProductId"]
                itemMetadata_RefId = x["RefId"]
                itemMetadata_Ean = x["Ean"]
                itemMetadata_ImageUrl = x["ImageUrl"]
                itemMetadata_DetailUrl = x["DetailUrl"]
        except:
            cache = 2
        
        try:
            courier = None
            invoiceNumber = None
            invoiceValue = None
            invoiceUrl = None
            issuanceDate = None
            trackingNumber = None
            invoiceKey = None
            trackingUrl = None
            embeddedInvoice = None
            packages_type = None
            cfop = None
            volumes = None
            EnableInferItems = None
            for x in packages:
                courier = x["courier"]
                invoiceNumber = x["invoiceNumber"]
                invoiceValue = x["invoiceValue"]
                invoiceUrl = x["invoiceUrl"]
                issuanceDate = x["issuanceDate"]
                trackingNumber = x["trackingNumber"]
                invoiceKey = x["invoiceKey"]
                trackingUrl = x["trackingUrl"]
                embeddedInvoice = x["embeddedInvoice"]
                packages_type = x["type"]
                cfop = x["cfop"]
                volumes = x["volumes"]
                EnableInferItems = x["EnableInferItems"]
        except:
            courier = None
            invoiceNumber = None
            invoiceValue = None
            invoiceUrl = None
            issuanceDate = None
            trackingNumber = None
            invoiceKey = None
            trackingUrl = None
            embeddedInvoice = None
            packages_type = None
            cfop = None
            volumes = None
            EnableInferItems = None
            
    
        try:
            dim_invoiceData = Fjson["invoiceData"]
            invoice_address = dim_invoiceData["address"]
            userPaymentInfo = dim_invoiceData["userPaymentInfo"]
        except:
            dim_invoiceData = None
            invoice_address = None
            userPaymentInfo = None
        
        try:    
            isActive = transactions["isActive"]
            transactionId = transactions["transactionId"]
            merchantName = transactions["merchantName"]
        except:
            cache = 2
            
        try:
            cancellationData = Fjson["cancellationData"]
            CancellationDate = cancellationData["CancellationDate"]
            RequestedByUser = cancellationData["RequestedByUser"]
            RequestedBySystem = cancellationData["RequestedBySystem"]
            RequestedBySellerNotification = cancellationData["RequestedBySellerNotification"]
            RequestedByPaymentNotification = cancellationData["RequestedByPaymentNotification"]
            Reason = cancellationData["Reason"]
        except:
            cancellationData = None
            RequestedByUser = None
            RequestedBySystem = None
            RequestedBySellerNotification = None
            RequestedByPaymentNotification = None
            Reason = None
            CancellationDate = None
        
        print(invoicedDate)
    
        df1 = pd.DataFrame({
            'orderId': str(id),
            'emailTracked': str(emailTracked),
            'approvedBy': str(approvedBy),
            'cancelledBy': str(cancelledBy),
            'cancelReason': str(cancelReason),
            'sequence': sequence,
            'marketplaceOrderId': str(marketplaceOrderId),
            'marketplaceServicesEndpoint': str(marketplaceServicesEndpoint),
            'sellerOrderId': str(sellerOrderId),
            'origin': str(origin),
            'affiliateId': str(affiliateId),
            'salesChannel': salesChannel,
            'merchantName': str(merchantName),
            'status': str(status),
            'statusDescription': str(statusDescription),
            #'value': value,
            #'creationDate': creationDate,
            #'lastChange': lastChange,
            #'orderGroup': orderGroup,
            #'giftRegistryData': str(giftRegistryData),
            #'callCenterOperatorData': str(callCenterOperatorData),
            #'followUpEmail': str(followUpEmail),
            #'lastMessage': str(lastMessage),
            #'hostname': str(hostname),
            #'openTextField': str(openTextField),
            #'roundingError': roundingError,
            #'orderFormId': str(orderFormId),
            #'commercialConditionData': str(commercialConditionData),
            #'isCompleted': isCompleted,
            #'customData': str(customData),
            #'allowCancellation': allowCancellation,
            #'allowEdition': allowEdition,
            #'isCheckedIn': isCheckedIn,
            #'authorizedDate': authorizedDate,
            #'DIM_TOTAL_id_items': str(total_id_items),
            #'DIM_TOTAL_name_items': str(total_name_items),
            #'DIM_TOTAL_value_items': total_value_items,
            #'DIM_TOTAL_id_discounts': str(total_id_discounts),
            #'DIM_TOTAL_name_discounts': str(total_name_discounts),
            #'DIM_TOTAL_value_discounts': total_value_discounts,
            #'DIM_TOTAL_id_shipping': str(total_id_shipping),
            #'DIM_TOTAL_name_shipping': str(total_name_shipping),
            #'DIM_TOTAL_value_shipping': total_value_shipping,
            #'DIM_TOTAL_id_tax': str(total_id_tax),
            #'DIM_TOTAL_name_tax': str(total_name_tax),
            #'DIM_TOTAL_value_tax': total_value_tax,
            #'DIM_ITEMS_uniqueId': str(items_uniqueId),
            #'DIM_ITEMS_items_id': items_id,
            #'DIM_ITEMS_productId': items_productId,
            #'DIM_ITEMS_ean': items_ean,
            #'DIM_ITEMS_lockId': str(items_lockId),
            #'DIM_ITEMS_quantity': item_quantity,
            #'DIM_ITEMS_seller': str(item_seller),
            #'DIM_ITEMS_name': str(item_name),
            #'DIM_ITEMS_refId': str(item_refId),
            #'DIM_ITEMS_price': item_price,
            #'DIM_ITEMS_listPrice': item_listPrice,
            #'DIM_ITEMS_manualPrice': str(item_manualPrice),
            #'DIM_ITEMS_imageUrl': str(item_imageUrl),
            #'DIM_ITEMS_detailUrl': str(item_detailUrl),
            #'DIM_ITEMS_sellerSku': item_sellerSku,
            #'DIM_ITEMS_priceValidUntil': item_priceValidUntil,
            #'DIM_ITEMS_commission': item_commission,
            #'DIM_ITEMS_tax': item_tax,
            #'DIM_ITEM_preSaleDate': str(item_preSaleDate),
            #'DIM_ITEM_measurementUnit': str(item_measurementUnit),
            #'DIM_ITEM_unitMultiplier': item_unitMultiplier,
            #'DIM_ITEM_sellingPrice': item_sellingPrice,
            #'DIM_ITEM_isGift': item_isGift,
            #'DIM_ITEM_shippingPrice': str(item_shippingPrice),
            #'DIM_ITEM_rewardValue': item_rewardValue,
            #'DIM_ITEM_freightCommission': item_freightCommission,
            #'DIM_ITEM_priceDefinition': str(item_price_definition),
            #'DIM_ITEM_taxCode': str(item_taxCode),
            #'DIM_ITEM_parentItemIndex': str(item_parentItemIndex),
            #'DIM_ITEM_parentAssemblyBinding': str(item_parentAssemblyBinding),
            #'DIM_ITEM_itemAttachment_name': str(item_itemAttachment_name),
            #'DIM_ITEM_AInfo_brandName': str(brandName),
            #'DIM_ITEM_AInfo_brandId': brandId,
            #'DIM_ITEM_AInfo_categoriesIds': str(categoriesIds),
            #'DIM_ITEM_AInfo_productClusterId': productClusterId,
            #'DIM_ITEM_AInfo_commercialConditionId': commercialConditionId,
            #'DIM_ITEM_AInfo_offeringInfo': str(offeringInfo),
            #'DIM_ITEM_AInfo_offeringType': str(offeringType),
            #'DIM_ITEM_AInfo_offeringTypeId': str(offeringTypeId),
            #'DIM_ITEM_AInfo_cubicweight': cubicweight,
            #'DIM_ITEM_AInfo_dim_height': height,
            #'DIM_ITEM_AInfo_dim_length': length,
            #'DIM_ITEM_AInfo_dim_weight': weight,
            #'DIM_ITEM_AInfo_dim_width': width,
            #'DIM_CLIENT': str(client_id),
            #'DIM_CLIENT_email': str(client_email),
            #'DIM_CLIENT_firstName': str(client_firstName),
            #'DIM_CLIENT_lastName': str(client_lastName),
            #'DIM_CLIENT_documentType': str(client_documentType),
            #'DIM_CLIENT_document': client_document,
            #'DIM_CLIENT_phone': client_phone,
            #'DIM_CLIENT_corporateName': str(client_corporateName),
            #'DIM_CLIENT_tradeName': str(client_tradeName),
            #'DIM_CLIENT_corporateDocument': str(client_corporateDocument),
            #'DIM_CLIENT_stateInscription': str(client_stateInscription),
            #'DIM_CLIENT_corporatePhone': str(client_corporatePhone),
            #'DIM_CLIENT_isCorporate': client_isCorporate,
            #'DIM_CLIENT_userProfileId': str(client_userProfileId),
            #'DIM_CLIENT_customerClass': str(client_customerClass),
            #'id_ratesAndBenefits': str(id_ratesAndBenefits),
            #'DIM_SHIPPING_DATA_shippingData_id': str(shippingData_id),
            #'DIM_SHIPPING_DATA_addressType': str(addressType),
            #'DIM_SHIPPING_DATA_receiverName': str(receiverName),
            #'DIM_SHIPPING_DATA_addressId': str(addressId),
            #'DIM_SHIPPING_DATA_postalCode': postalCode,
            #'DIM_SHIPPING_DATA_city': str(city),
            #'DIM_SHIPPING_DATA_state': str(state),
            #'DIM_SHIPPING_DATA_country': str(country),
            #'DIM_SHIPPING_DATA_street': str(street),
            #'DIM_SHIPPING_DATA_number': str(number),
            #'DIM_SHIPPING_DATA_neighborhood': str(neighborhood),
            #'DIM_SHIPPING_DATA_complement': str(complement),
            #'DIM_SHIPPING_DATA_reference': str(reference),
            #'DIM_SHIPPING_DATA_deliveryChannel': str(deliveryChannel),
            #'DIM_SHIPPING_DATA_addressId': str(addressId),
            #'DIM_SHIPPING_DATA_polygonName': str(polygonName),
            #'DIM_SHIPPING_DATA_itemIndex': str(itemIndex),
            #'DIM_SHIPPING_DATA_selectedSla': str(selectedSla),
            #'DIM_SHIPPING_DATA_lockTTL': str(lockTTL),
            #'DIM_SHIPPING_DATA_price': price,
            #'DIM_SHIPPING_DATA_listPrice': listPrice,
            #'DIM_SHIPPING_DATA_sellingPrice': sellingPrice,
            #'DIM_SHIPPING_DATA_deliveryCompany': str(deliveryCompany),
            #'DIM_SHIPPING_DATA_shippingEstimate': str(shippingEstimate),
            #'DIM_SHIPPING_DATA_shippingEstimateDate': shippingEstimateDate,
            #'DIM_SHIPPING_DATA_slas_id': str(slas_id),
            #'DIM_SHIPPING_DATA_slas_name': str(slas_name),
            #'DIM_SHIPPING_DATA_slas_shippingEstimate': str(slas_shippingEstimate),
            #'DIM_SHIPPING_DATA_slas_price': slas_price,
            #'DIM_SHIPPING_DATA_slas_deliveryChannel': str(slas_deliveryChannel),
            #'DIM_SHIPPING_DATA_slas_polygonName': str(slas_polygonName),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo': str(slas_pickupStoreInfo_additionalInfo),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId': str(slas_pickupStoreInfo_dockId),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName': str(slas_pickupStoreInfo_friendlyName),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore': slas_pickupStoreInfo_isPickupStore,
            #'DIM_SHIPPING_DATA_slas_id_01': str(slas_id_01),
            #'DIM_SHIPPING_DATA_slas_name_01': str(slas_name_01),
            #'DIM_SHIPPING_DATA_slas_shippingEstimate_01': str(slas_shippingEstimate_01),
            #'DIM_SHIPPING_DATA_slas_price_01': slas_price_01,
            #'DIM_SHIPPING_DATA_slas_deliveryChannel_01': slas_deliveryChannel_01,
            #'DIM_SHIPPING_DATA_slas_polygonName_01': str(slas_polygonName_01),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_01': str(slas_pickupStoreInfo_additionalInfo_01),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_01': str(slas_pickupStoreInfo_dockId_01),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_01': str(slas_pickupStoreInfo_friendlyName_01),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_01': slas_pickupStoreInfo_isPickupStore_01,
            #'DIM_SHIPPING_DATA_slas_id_02': str(slas_id_02),
            #'DIM_SHIPPING_DATA_slas_name_02': str(slas_name_02),
            #'DIM_SHIPPING_DATA_slas_shippingEstimate_02': str(slas_shippingEstimate_02),
            #'DIM_SHIPPING_DATA_slas_deliveryWindow_02': str(slas_deliveryWindow_02),
            #'DIM_SHIPPING_DATA_slas_price_02': slas_price_02,
            #'DIM_SHIPPING_DATA_slas_deliveryChannel_02': str(slas_deliveryChannel_02),
            #'DIM_SHIPPING_DATA_slas_polygonName_02': str(slas_polygonName_02),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_02': str(slas_pickupStoreInfo_dockId_02),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_02': str(slas_pickupStoreInfo_friendlyName_02),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_02': slas_pickupStoreInfo_isPickupStore_02,
            #'DIM_SHIPPING_DATA_slas_id_03': str(slas_id_03),
            #'DIM_SHIPPING_DATA_slas_name_03': str(slas_name_03),
            #'DIM_SHIPPING_DATA_slas_shippingEstimate_03': str(slas_shippingEstimate_03),
            #'DIM_SHIPPING_DATA_slas_price_03': slas_price_03,
            #'DIM_SHIPPING_DATA_slas_deliveryChannel_03': str(slas_deliveryChannel_03),
            #'DIM_SHIPPING_DATA_slas_polygonName_03': str(slas_polygonName_03),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_03': str(slas_pickupStoreInfo_additionalInfo_03),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_03': str(slas_pickupStoreInfo_dockId_03),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_03': str(slas_pickupStoreInfo_friendlyName_03),
            #'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_03': slas_pickupStoreInfo_isPickupStore_03,
            #'DIM_SHIPPING_DATA_courierId_delivery': str(courierId),
            #'DIM_SHIPPING_DATA_courierName_delivery': str(courierName),
            #'DIM_SHIPPING_DATA_dockId_delivery': dockId,
            #'DIM_SHIPPING_DATA_quantity_delivery': quantity,
            #'DIM_SHIPPING_DATA_warehouseId': str(warehouseId),
            #'DIM_SHIPPING_DATA_pickupStoreInfo_additionalInfo': str(pickupStoreInfo_additionalInfo),
            #'DIM_SHIPPING_DATA_pickupStoreInfo_dockId': str(pickupStoreInfo_dockId),
            #'DIM_SHIPPING_DATA_pickupStoreInfo_friendlyName': str(pickupStoreInfo_friendlyName),
            #'DIM_SHIPPING_DATA_pickupStoreInfo_isPickupStore': pickupStoreInfo_isPickupStore,
            #'DIM_SHIPPING_DATA_selectedAddresses_addressId': str(selectedAddresses_addressId),
            #'DIM_SHIPPING_DATA_selectedAddresses_addressType': str(selectedAddresses_addressType),
            #'DIM_SHIPPING_DATA_selectedAddresses_receiverName': selectedAddresses_receiverName,
            #'DIM_SHIPPING_DATA_selectedAddresses_street': str(selectedAddresses_street),
            #'DIM_SHIPPING_DATA_selectedAddresses_number': str(selectedAddresses_number),
            #'DIM_SHIPPING_DATA_selectedAddresses_complement': str(selectedAddresses_complement),
            #'DIM_SHIPPING_DATA_selectedAddresses_neighborhood': str(selectedAddresses_neighborhood),
            #'DIM_SHIPPING_DATA_selectedAddresses_postalCode': selectedAddresses_postalCode,
            #'DIM_SHIPPING_DATA_selectedAddresses_city': str(selectedAddresses_city),
            #'DIM_SHIPPING_DATA_selectedAddresses_state': str(selectedAddresses_state),
            #'DIM_SHIPPING_DATA_selectedAddresses_country': str(selectedAddresses_country),
            #'DIM_SHIPPING_DATA_selectedAddresses_reference': str(selectedAddresses_reference),
            #'transactions_isActive': transactions_isActive,
            #'transactions_transactionId': str(transactions_transactionId),
            #'transactions_merchantName': str(transactions_merchantName),
            #'payments_id': str(payments_id),
            #'payments_paymentSystem': payments_paymentSystem,
            #'payments_paymentSystemName': str(payments_paymentSystemName),
            #'payments_value': payments_value,
            #'payments_installments': payments_installments,
            #'payments_referenceValue': payments_referenceValue,
            #'payments_cardHolder': str(payments_cardHolder),
            #'payments_firstDigits': payments_firstDigits,
            #'payments_lastDigits': payments_lastDigits,
            #'payments_url': str(payments_url),
            #'payments_giftCardId': str(payments_giftCardId),
            #'payments_giftCardName': str(payments_giftCardName),
            #'payments_giftCardCaption': str(payments_giftCardCaption),
            #'payments_redemptionCode': str(payments_redemptionCode),
            #'payments_group': str(payments_group),
            #'payments_dueDate': str(payments_dueDate),
            #'payments_cardNumber': str(payments_cardNumber),
            #'payments_cvv2': str(payments_cvv2),
            #'payments_expireMonth': str(payments_expireMonth),
            #'payments_expireYear': str(payments_expireYear),
            #'payments_giftCardProvider': str(payments_giftCardProvider),
            #'payments_giftCardAsDiscount': str(payments_giftCardAsDiscount),
            #'payments_koinUrl': str(payments_koinUrl),
            #'payments_accountId': str(payments_accountId),
            #'payments_parentAccountId': str(payments_parentAccountId),
            #'payments_bankIssuedInvoiceIdentificationNumber': str(payments_bankIssuedInvoiceIdentificationNumber),
            #'payments_bankIssuedInvoiceIdentificationNumberFormatted': str(payments_bankIssuedInvoiceIdentificationNumberFormatted),
            #'payments_bankIssuedInvoiceBarCodeNumber': str(payments_bankIssuedInvoiceBarCodeNumber),
            #'payments_bankIssuedInvoiceBarCodeType': str(payments_bankIssuedInvoiceBarCodeType),
            #'payments_Tid': str(payments_tid),
            #'payments_ReturnCode': payments_ReturnCode,
            #'payments_Message': str(payments_Message),
            #'payments_authId': str(payments_authId),
            #'payments_acquirer': str(payments_acquirer),
            #'billingAddress_postalCode': billingAddress_postalCode,
            #'billingAddress_city': str(billingAddress_city),
            #'billingAddress_state': str(billingAddress_state),
            #'billingAddress_country': str(billingAddress_country),
            #'billingAddress_street': str(billingAddress_street),
            #'billingAddress_number': billingAddress_number,
            #'billingAddress_neighborhood': str(billingAddress_neighborhood),
            #'billingAddress_complement': str(billingAddress_complement),
            #'billingAddress_reference': str(billingAddress_reference),
            #'seller_id': str(seller_id),
            #'seller_name': str(seller_name),
            #'seller_logo': str(seller_logo),
            #'changesAttachment_id': str(changesAttachment_id),
            #'storePreferencesData_countryCode': str(storePreferencesData_countryCode),
            #'storePreferencesData_currencyCode': str(storePreferencesData_currencyCode),
            #'storePreferencesData_currencyLocale': storePreferencesData_currencyLocale,
            #'storePreferencesData_currencySymbol': str(storePreferencesData_currencySymbol),
            #'storePreferencesData_timeZone': str(storePreferencesData_timeZone),
            #'CurrencyDecimalDigits': CurrencyDecimalDigits,
            #'CurrencyDecimalSeparator': str(CurrencyDecimalSeparator),
            #'CurrencyGroupSeparator': str(CurrencyGroupSeparator),
            #'CurrencyGroupSize': CurrencyGroupSize,
            #'StartsWithCurrencySymbol': StartsWithCurrencySymbol,
            #'marketplace_baseURL': str(baseURL),
            #'marketplace_isCertified': str(isCertified),
            #'marketplace_name': str(name),
            #'itemMetadata_Id': itemMetadata_Id,
            #'itemMetadata_Seller': str(itemMetadata_Seller),
            #'itemMetadata_Name': str(itemMetadata_Name),
            #'itemMetadata_SkuName': str(itemMetadata_SkuName),
            #'itemMetadata_ProductId': itemMetadata_ProductId,
            #'itemMetadata_RefId': itemMetadata_RefId,
            #'itemMetadata_Ean': itemMetadata_Ean,
            #'itemMetadata_ImageUrl': str(itemMetadata_ImageUrl),
            #'itemMetadata_DetailUrl': str(itemMetadata_DetailUrl),
            #'subscriptionData': str(subscriptionData),
            #'taxData': str(taxData),
            #'courier': str(courier),
            #'invoiceNumber': str(invoiceNumber),
            #'invoiceValue': str(invoiceValue),
            #'invoiceUrl': str(invoiceUrl),
            #'issuanceDate': issuanceDate,
            #'trackingNumber': str(trackingNumber),
            #'invoiceKey': str(invoiceKey),
            #'trackingUrl': str(trackingUrl),
            #'embeddedInvoice': embeddedInvoice,
            #'type': str(packages_type),
            #'cfop': str(cfop),
            #'volumes': str(volumes),
            #'EnableInferItems': str(EnableInferItems),
            #'invoice_address': str(invoice_address),
            #'userPaymentInfo': str(userPaymentInfo),
            #'serialNumbers':str(item_serialNumbers),
            #'isActive':isActive,
            #'transactionId':str(transactionId),
            #'merchantName':str(merchantName),
            #'RequestedByUser':str(RequestedByUser),
            #'RequestedBySystem':str(RequestedBySystem),
            #'RequestedBySellerNotification':str(RequestedBySellerNotification),
            #'RequestedByPaymentNotification':str(RequestedByPaymentNotification),
            #'Reason':str(Reason),
            #'CancellationDate':str(CancellationDate),
            'invoicedDate': invoicedDate}, index=[0])
        init.df = init.df.append(df1)
    except:
        cache = 2

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
    
    table_schema = [
  {
    "name": "CancellationDate",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "Reason",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "RequestedBySystem",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "isActive",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "RequestedBySellerNotification",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "userPaymentInfo",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "EnableInferItems",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "volumes",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "type",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "embeddedInvoice",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "trackingUrl",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "issuanceDate",
    "type": "DATE",
    "mode": "NULLABLE"
  },
  {
    "name": "invoiceUrl",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "courier",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "subscriptionData",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "itemMetadata_DetailUrl",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "itemMetadata_ProductId",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "itemMetadata_Id",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "marketplace_name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "marketplace_isCertified",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "marketplace_baseURL",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "CurrencyDecimalSeparator",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "CurrencyDecimalDigits",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "storePreferencesData_timeZone",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "storePreferencesData_currencySymbol",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "storePreferencesData_currencyCode",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "storePreferencesData_countryCode",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "RequestedByUser",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "seller_logo",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "seller_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "billingAddress_reference",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "billingAddress_complement",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "billingAddress_neighborhood",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "billingAddress_number",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "billingAddress_street",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "billingAddress_country",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "billingAddress_state",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "billingAddress_city",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_acquirer",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "itemMetadata_Name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_authId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_Message",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "itemMetadata_RefId",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "storePreferencesData_currencyLocale",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_ReturnCode",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_Tid",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_bankIssuedInvoiceBarCodeType",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_bankIssuedInvoiceBarCodeNumber",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "itemMetadata_Seller",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_bankIssuedInvoiceIdentificationNumberFormatted",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_bankIssuedInvoiceIdentificationNumber",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_parentAccountId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_giftCardProvider",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_expireMonth",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_cvv2",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_group",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_redemptionCode",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_giftCardCaption",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_giftCardName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_giftCardId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_url",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_lastDigits",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "invoicedDate",
    "type": "DATE",
    "mode": "NULLABLE"
  },
  {
    "name": "itemMetadata_ImageUrl",
    "type": "DATE",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_firstDigits",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_value",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_paymentSystemName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedAddresses_reference",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_postalCode",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "transactions_transactionId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedAddresses_postalCode",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "origin",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedAddresses_complement",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedAddresses_number",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedAddresses_street",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedAddresses_addressType",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_listPrice",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedAddresses_addressId",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_01",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_02",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_pickupStoreInfo_friendlyName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_dockId_delivery",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_courierName_delivery",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_courierId_delivery",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "invoiceNumber",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_03",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_price_03",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_name_03",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_shippingEstimate_03",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "salesChannel",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_id_01",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_polygonName_02",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_accountId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_deliveryWindow_02",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_name_02",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_stateInscription",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_01",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_referenceValue",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_productClusterId",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_pickupStoreInfo_additionalInfo",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_giftCardAsDiscount",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_polygonName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_quantity_delivery",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_deliveryChannel_01",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_TOTAL_value_shipping",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_price_01",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "CurrencyGroupSeparator",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_isCorporate",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_polygonName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_deliveryChannel",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_price",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_shippingPrice",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_shippingEstimate",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "trackingNumber",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "customData",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_sellingPrice",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_itemIndex",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_rewardValue",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_deliveryChannel",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_parentAssemblyBinding",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_complement",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_neighborhood",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_number",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_id_03",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "transactions_isActive",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_dim_length",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_shippingData_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_country",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "cancelReason",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_state",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_addressId",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_name_01",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_addressType",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_customerClass",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "RequestedByPaymentNotification",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_userProfileId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedAddresses_receiverName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_phone",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_02",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_corporatePhone",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "invoiceValue",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_corporateDocument",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_tradeName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_shippingEstimate",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "emailTracked",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_TOTAL_name_tax",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_corporateName",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_price_02",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "serialNumbers",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_TOTAL_id_discounts",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "invoice_address",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_TOTAL_id_tax",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_sellingPrice",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_lastName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_warehouseId",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_dim_width",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_city",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_cardNumber",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_productId",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_dim_height",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "transactionId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "merchantName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_taxCode",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_refId",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_commercialConditionId",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_itemAttachment_name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_offeringType",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "taxData",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "hostname",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_offeringTypeId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_pickupStoreInfo_isPickupStore",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_document",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_id_02",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "statusDescription",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_lockId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_receiverName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_03",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "billingAddress_postalCode",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_quantity",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_deliveryChannel_03",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_reference",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_01",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "roundingError",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_dim_weight",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_isGift",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_deliveryChannel_02",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_02",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_tax",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_installments",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "value",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_pickupStoreInfo_dockId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_sellerSku",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "authorizedDate",
    "type": "DATE",
    "mode": "NULLABLE"
  },
  {
    "name": "sequence",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_email",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "changesAttachment_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_listPrice",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "lastMessage",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "transactions_merchantName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "itemMetadata_Ean",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_detailUrl",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_seller",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_manualPrice",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_price",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "creationDate",
    "type": "DATE",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_dueDate",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_ean",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_commission",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_paymentSystem",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_TOTAL_value_tax",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedSla",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "followUpEmail",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_TOTAL_name_shipping",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_TOTAL_id_items",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "marketplaceServicesEndpoint",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_street",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_TOTAL_id_shipping",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_unitMultiplier",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_TOTAL_value_discounts",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "invoiceKey",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "orderGroup",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_koinUrl",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_TOTAL_name_discounts",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_TOTAL_name_items",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_deliveryCompany",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_preSaleDate",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_items_id",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "CurrencyGroupSize",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "cancelledBy",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "allowEdition",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_categoriesIds",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_brandId",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "allowCancellation",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_documentType",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_01",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "cfop",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "commercialConditionData",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "status",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "orderFormId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_polygonName_01",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_shippingEstimate_01",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "isCheckedIn",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_imageUrl",
    "type": "DATE",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_cardHolder",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedAddresses_neighborhood",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "isCompleted",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "openTextField",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedAddresses_city",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_measurementUnit",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "approvedBy",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_uniqueId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_polygonName_03",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "payments_expireYear",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "sellerOrderId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_03",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT_firstName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_CLIENT",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_brandName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "id_ratesAndBenefits",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_TOTAL_value_items",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "giftRegistryData",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_03",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "lastChange",
    "type": "DATE",
    "mode": "NULLABLE"
  },
  {
    "name": "callCenterOperatorData",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_cubicweight",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedAddresses_state",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEMS_priceValidUntil",
    "type": "DATE",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_priceDefinition",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_AInfo_offeringInfo",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "marketplaceOrderId",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "itemMetadata_SkuName",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "StartsWithCurrencySymbol",
    "type": "BOOLEAN",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_selectedAddresses_country",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_slas_shippingEstimate_02",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_lockTTL",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_parentItemIndex",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_price",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_ITEM_freightCommission",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "affiliateId",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "seller_name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "DIM_SHIPPING_DATA_shippingEstimateDate",
    "type": "DATE",
    "mode": "NULLABLE"
  },
  {
    "name": "orderId",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
    
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_vtex_order_01'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    #job_config.write_disposition = "WRITE_TRUNCATE"
    #job_config.autodetect = True
    job_config.schema = format_schema(table_schema)
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()        


def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
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