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
    #try:
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
            cache = 2
        try:
            slas_pickupStoreInfo_additionalInfo_01 = pickupStoreInfo_1["additionalInfo"]
            slas_pickupStoreInfo_address_01 = pickupStoreInfo_1["address"]
            slas_pickupStoreInfo_dockId_01 = pickupStoreInfo_1["dockId"]
            slas_pickupStoreInfo_friendlyName_01 = pickupStoreInfo_1["friendlyName"]
            slas_pickupStoreInfo_isPickupStore_01 = pickupStoreInfo_1["isPickupStore"]
        except:
            cache = 2
        
        try:
            slas_id_02 = slas_2["id"]
            slas_name_02 = slas_2["name"]
            slas_shippingEstimate_02 = slas_2["shippingEstimate"]
            slas_deliveryWindow_02 = slas_2["deliveryWindow"]
            slas_price_02 = slas_2["price"]
            slas_deliveryChannel_02 = slas_2["deliveryChannel"]
            slas_polygonName_02 = slas_2["polygonName"]
        except:
            cache = 2
        try:
            slas_pickupStoreInfo_additionalInfo_02 = pickupStoreInfo_2["additionalInfo"]
            slas_pickupStoreInfo_address_02 = pickupStoreInfo_2["address"]
            slas_pickupStoreInfo_dockId_02 = pickupStoreInfo_2["dockId"]
            slas_pickupStoreInfo_friendlyName_02 = pickupStoreInfo_2["friendlyName"]
            slas_pickupStoreInfo_isPickupStore_02 = pickupStoreInfo_2["isPickupStore"]
        except:
            cache = 2
            
        try:
            slas_id_03 = slas_3["id"]
            slas_name_03 = slas_3["name"]
            slas_shippingEstimate_03 = slas_3["shippingEstimate"]
            slas_deliveryWindow_03 = slas_3["deliveryWindow"]
            slas_price_03 = slas_3["price"]
            slas_deliveryChannel_03 = slas_3["deliveryChannel"]
            slas_polygonName_03 = slas_3["polygonName"]
        except:
            cache = 2
        try: 
            slas_pickupStoreInfo_additionalInfo_03 = pickupStoreInfo_3["additionalInfo"]
            slas_pickupStoreInfo_address_03 = pickupStoreInfo_3["address"]
            slas_pickupStoreInfo_dockId_03 = pickupStoreInfo_3["dockId"]
            slas_pickupStoreInfo_friendlyName_03 = pickupStoreInfo_3["friendlyName"]
            slas_pickupStoreInfo_isPickupStore_03 = pickupStoreInfo_3["isPickupStore"]
        except:
            cache = 2
        try:
            courierId = deliveryIds["courierId"]
            courierName = deliveryIds["courierName"]
            dockId = deliveryIds["dockId"]
            quantity = deliveryIds["quantity"]
            warehouseId = deliveryIds["warehouseId"]
        except:
            cache = 2
        try: 
            pickupStoreInfo_additionalInfo = pickupStoreInfo["additionalInfo"]
            pickupStoreInfo_address = pickupStoreInfo["address"]
            pickupStoreInfo_dockId = pickupStoreInfo["dockId"]
            pickupStoreInfo_friendlyName = pickupStoreInfo["friendlyName"]
            pickupStoreInfo_isPickupStore = pickupStoreInfo["isPickupStore"]
        except:
            cache = 2
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
            cache = 2
        try:
            transactions_isActive = transactions["isActive"]
            transactions_transactionId = transactions["transactionId"]
            transactions_merchantName = transactions["merchantName"]
        except:
            cache = 2
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
            payments_tid = payments["tid"]
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
            connectorResponses = payments["connectorResponses"]
            payments_ReturnCode = connectorResponses["ReturnCode"]
            payments_Message = connectorResponses["Message"]
            payments_authId = connectorResponses["authId"]
            payments_acquirer = connectorResponses["acquirer"]
        except:
            cache = 2
        try:
            billingAddress_postalCode = billingAddress["postalCode"]
            billingAddress_city = billingAddress["city"]
            billingAddress_state = billingAddress["state"]
            billingAddress_country = billingAddress["country"]
            billingAddress_street = billingAddress["street"]
            billingAddress_number = billingAddress["number"]
            billingAddress_neighborhood = billingAddress["neighborhood"]
            billingAddress_complement = billingAddress["complement"]
            billingAddress_reference = billingAddress["reference"]
        except:
            cache = 2
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
            cache = 2
        
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
                type = x["type"]
                courierStatus = x["courierStatus"]
                cfop = x["cfop"]
                restitutions = packages["restitutions"]
                volumes = x["volumes"]
                EnableInferItems = x["EnableInferItems"]
        except:
            cache = 2
            
    
        try:
            dim_invoiceData = Fjson["invoiceData"]
            invoice_address = dim_invoiceData["address"]
            userPaymentInfo = dim_invoiceData["userPaymentInfo"]
        except:
            cache = 2
        
        try:    
            isActive = transactions["isActive"]
            transactionId = transactions["transactionId"]
            merchantName = transactions["merchantName"]
        except:
            cache = 2
            
        try:
            cancellationData = Fjson["cancellationData"]
            RequestedByUser = cancellationData["RequestedByUser"]
            RequestedBySystem = cancellationData["RequestedBySystem"]
            RequestedBySellerNotification = cancellationData["RequestedBySellerNotification"]
            RequestedByPaymentNotification = cancellationData["RequestedByPaymentNotification"]
            Reason = cancellationData["Reason"]
        except:
            cache = 2
    
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
            'value': value,
            'creationDate': creationDate,
            'lastChange': lastChange,
            'orderGroup': orderGroup,
            'giftRegistryData': str(giftRegistryData),
            'callCenterOperatorData': str(callCenterOperatorData),
            'followUpEmail': str(followUpEmail),
            'lastMessage': str(lastMessage),
            'hostname': str(hostname),
            'openTextField': str(openTextField),
            'roundingError': roundingError,
            'orderFormId': str(orderFormId),
            'commercialConditionData': str(commercialConditionData),
            'isCompleted': isCompleted,
            'customData': str(customData),
            'allowCancellation': allowCancellation,
            'allowEdition': allowEdition,
            'isCheckedIn': isCheckedIn,
            'authorizedDate': authorizedDate,
            'DIM_TOTAL_id_items': str(total_id_items),
            'DIM_TOTAL_name_items': str(total_name_items),
            'DIM_TOTAL_value_items': total_value_items,
            'DIM_TOTAL_id_discounts': str(total_id_discounts),
            'DIM_TOTAL_name_discounts': str(total_name_discounts),
            'DIM_TOTAL_value_discounts': total_value_discounts,
            'DIM_TOTAL_id_shipping': str(total_id_shipping),
            'DIM_TOTAL_name_shipping': str(total_name_shipping),
            'DIM_TOTAL_value_shipping': total_value_shipping,
            'DIM_TOTAL_id_tax': str(total_id_tax),
            'DIM_TOTAL_name_tax': str(total_name_tax),
            'DIM_TOTAL_value_tax': total_value_tax,
            'DIM_TOTAL_id_change': str(total_id_change),
            'DIM_TOTAL_name_change': str(total_name_change),
            'DIM_TOTAL_value_change': str(total_value_change),
            'DIM_ITEMS_uniqueId': str(items_uniqueId),
            'DIM_ITEMS_items_id': items_id,
            'DIM_ITEMS_productId': items_productId,
            'DIM_ITEMS_ean': items_ean,
            'DIM_ITEMS_lockId': str(items_lockId),
            'DIM_ITEMS_quantity': item_quantity,
            'DIM_ITEMS_seller': str(item_seller),
            'DIM_ITEMS_name': str(item_name),
            'DIM_ITEMS_refId': str(item_refId),
            'DIM_ITEMS_price': item_price,
            'DIM_ITEMS_listPrice': item_listPrice,
            'DIM_ITEMS_manualPrice': str(item_manualPrice),
            'DIM_ITEMS_imageUrl': str(item_imageUrl),
            'DIM_ITEMS_detailUrl': str(item_detailUrl),
            'DIM_ITEMS_sellerSku': item_sellerSku,
            'DIM_ITEMS_priceValidUntil': item_priceValidUntil,
            'DIM_ITEMS_commission': item_commission,
            'DIM_ITEMS_tax': item_tax,
            'DIM_ITEM_preSaleDate': str(item_preSaleDate),
            'DIM_ITEM_measurementUnit': str(item_measurementUnit),
            'DIM_ITEM_unitMultiplier': item_unitMultiplier,
            'DIM_ITEM_sellingPrice': item_sellingPrice,
            'DIM_ITEM_isGift': item_isGift,
            'DIM_ITEM_shippingPrice': str(item_shippingPrice),
            'DIM_ITEM_rewardValue': item_rewardValue,
            'DIM_ITEM_freightCommission': item_freightCommission,
            'DIM_ITEM_priceDefinition': str(item_price_definition),
            'DIM_ITEM_taxCode': str(item_taxCode),
            'DIM_ITEM_parentItemIndex': str(item_parentItemIndex),
            'DIM_ITEM_parentAssemblyBinding': str(item_parentAssemblyBinding),
            'DIM_ITEM_itemAttachment_name': str(item_itemAttachment_name),
            'DIM_ITEM_AInfo_brandName': str(brandName),
            'DIM_ITEM_AInfo_brandId': brandId,
            'DIM_ITEM_AInfo_categoriesIds': str(categoriesIds),
            'DIM_ITEM_AInfo_productClusterId': productClusterId,
            'DIM_ITEM_AInfo_commercialConditionId': commercialConditionId,
            'DIM_ITEM_AInfo_offeringInfo': str(offeringInfo),
            'DIM_ITEM_AInfo_offeringType': str(offeringType),
            'DIM_ITEM_AInfo_offeringTypeId': str(offeringTypeId),
            'DIM_ITEM_AInfo_cubicweight': cubicweight,
            'DIM_ITEM_AInfo_dim_height': height,
            'DIM_ITEM_AInfo_dim_length': length,
            'DIM_ITEM_AInfo_dim_weight': weight,
            'DIM_ITEM_AInfo_dim_width': width,
            'DIM_ITEM_calculatedSellingPrice': str(calculatedSellingPrice),
            'DIM_ITEM_priceDefinition_total': str(total),
            'DIM_CLIENT': str(client_id),
            'DIM_CLIENT_email': str(client_email),
            'DIM_CLIENT_firstName': str(client_firstName),
            'DIM_CLIENT_lastName': str(client_lastName),
            'DIM_CLIENT_documentType': str(client_documentType),
            'DIM_CLIENT_document': client_document,
            'DIM_CLIENT_phone': client_phone,
            'DIM_CLIENT_corporateName': str(client_corporateName),
            'DIM_CLIENT_tradeName': str(client_tradeName),
            'DIM_CLIENT_corporateDocument': str(client_corporateDocument),
            'DIM_CLIENT_stateInscription': str(client_stateInscription),
            'DIM_CLIENT_corporatePhone': str(client_corporatePhone),
            'DIM_CLIENT_isCorporate': client_isCorporate,
            'DIM_CLIENT_userProfileId': str(client_userProfileId),
            'DIM_CLIENT_customerClass': str(client_customerClass),
            'id_ratesAndBenefits': str(id_ratesAndBenefits),
            'DIM_SHIPPING_DATA_shippingData_id': str(shippingData_id),
            'DIM_SHIPPING_DATA_addressType': str(addressType),
            'DIM_SHIPPING_DATA_receiverName': str(receiverName),
            'DIM_SHIPPING_DATA_addressId': str(addressId),
            'DIM_SHIPPING_DATA_postalCode': postalCode,
            'DIM_SHIPPING_DATA_city': str(city),
            'DIM_SHIPPING_DATA_state': str(state),
            'DIM_SHIPPING_DATA_country': str(country),
            'DIM_SHIPPING_DATA_street': str(street),
            'DIM_SHIPPING_DATA_number': str(number),
            'DIM_SHIPPING_DATA_neighborhood': str(neighborhood),
            'DIM_SHIPPING_DATA_complement': str(complement),
            'DIM_SHIPPING_DATA_reference': str(reference),
            'DIM_SHIPPING_DATA_deliveryChannel': str(deliveryChannel),
            'DIM_SHIPPING_DATA_addressId': str(addressId),
            'DIM_SHIPPING_DATA_polygonName': str(polygonName),
            'DIM_SHIPPING_DATA_itemIndex': str(itemIndex),
            'DIM_SHIPPING_DATA_selectedSla': str(selectedSla),
            'DIM_SHIPPING_DATA_lockTTL': str(lockTTL),
            'DIM_SHIPPING_DATA_price': price,
            'DIM_SHIPPING_DATA_listPrice': listPrice,
            'DIM_SHIPPING_DATA_sellingPrice': sellingPrice,
            'DIM_SHIPPING_DATA_deliveryCompany': str(deliveryCompany),
            'DIM_SHIPPING_DATA_shippingEstimate': str(shippingEstimate),
            'DIM_SHIPPING_DATA_shippingEstimateDate': shippingEstimateDate,
            'DIM_SHIPPING_DATA_slas_id': str(slas_id),
            'DIM_SHIPPING_DATA_slas_name': str(slas_name),
            'DIM_SHIPPING_DATA_slas_shippingEstimate': str(slas_shippingEstimate),
            'DIM_SHIPPING_DATA_slas_price': slas_price,
            'DIM_SHIPPING_DATA_slas_deliveryChannel': str(slas_deliveryChannel),
            'DIM_SHIPPING_DATA_slas_polygonName': str(slas_polygonName),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo': str(slas_pickupStoreInfo_additionalInfo),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId': str(slas_pickupStoreInfo_dockId),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName': str(slas_pickupStoreInfo_friendlyName),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore': slas_pickupStoreInfo_isPickupStore,
            'DIM_SHIPPING_DATA_slas_id_01': str(slas_id_01),
            'DIM_SHIPPING_DATA_slas_name_01': str(slas_name_01),
            'DIM_SHIPPING_DATA_slas_shippingEstimate_01': str(slas_shippingEstimate_01),
            'DIM_SHIPPING_DATA_slas_price_01': slas_price_01,
            'DIM_SHIPPING_DATA_slas_deliveryChannel_01': slas_deliveryChannel_01,
            'DIM_SHIPPING_DATA_slas_polygonName_01': str(slas_polygonName_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_01': str(slas_pickupStoreInfo_additionalInfo_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_01': str(slas_pickupStoreInfo_dockId_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_01': str(slas_pickupStoreInfo_friendlyName_01),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_01': slas_pickupStoreInfo_isPickupStore_01,
            'DIM_SHIPPING_DATA_slas_id_02': str(slas_id_02),
            'DIM_SHIPPING_DATA_slas_name_02': str(slas_name_02),
            'DIM_SHIPPING_DATA_slas_shippingEstimate_02': str(slas_shippingEstimate_02),
            'DIM_SHIPPING_DATA_slas_deliveryWindow_02': str(slas_deliveryWindow_02),
            'DIM_SHIPPING_DATA_slas_price_02': slas_price_02,
            'DIM_SHIPPING_DATA_slas_deliveryChannel_02': str(slas_deliveryChannel_02),
            'DIM_SHIPPING_DATA_slas_polygonName_02': str(slas_polygonName_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_02': str(slas_pickupStoreInfo_additionalInfo_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_02': str(slas_pickupStoreInfo_dockId_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_02': str(slas_pickupStoreInfo_friendlyName_02),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_02': slas_pickupStoreInfo_isPickupStore_02,
            'DIM_SHIPPING_DATA_slas_id_03': str(slas_id_03),
            'DIM_SHIPPING_DATA_slas_name_03': str(slas_name_03),
            'DIM_SHIPPING_DATA_slas_shippingEstimate_03': str(slas_shippingEstimate_03),
            'DIM_SHIPPING_DATA_slas_price_03': slas_price_03,
            'DIM_SHIPPING_DATA_slas_deliveryChannel_03': str(slas_deliveryChannel_03),
            'DIM_SHIPPING_DATA_slas_polygonName_03': str(slas_polygonName_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_03': str(slas_pickupStoreInfo_additionalInfo_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_03': str(slas_pickupStoreInfo_dockId_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_03': str(slas_pickupStoreInfo_friendlyName_03),
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_03': slas_pickupStoreInfo_isPickupStore_03,
            'DIM_SHIPPING_DATA_courierId_delivery': str(courierId),
            'DIM_SHIPPING_DATA_courierName_delivery': str(courierName),
            'DIM_SHIPPING_DATA_dockId_delivery': dockId,
            'DIM_SHIPPING_DATA_quantity_delivery': quantity,
            'DIM_SHIPPING_DATA_warehouseId': str(warehouseId),
            'DIM_SHIPPING_DATA_pickupStoreInfo_additionalInfo': str(pickupStoreInfo_additionalInfo),
            'DIM_SHIPPING_DATA_pickupStoreInfo_dockId': str(pickupStoreInfo_dockId),
            'DIM_SHIPPING_DATA_pickupStoreInfo_friendlyName': str(pickupStoreInfo_friendlyName),
            'DIM_SHIPPING_DATA_pickupStoreInfo_isPickupStore': pickupStoreInfo_isPickupStore,
            'DIM_SHIPPING_DATA_selectedAddresses_addressId': str(selectedAddresses_addressId),
            'DIM_SHIPPING_DATA_selectedAddresses_addressType': str(selectedAddresses_addressType),
            'DIM_SHIPPING_DATA_selectedAddresses_receiverName': selectedAddresses_receiverName,
            'DIM_SHIPPING_DATA_selectedAddresses_street': str(selectedAddresses_street),
            'DIM_SHIPPING_DATA_selectedAddresses_number': str(selectedAddresses_number),
            'DIM_SHIPPING_DATA_selectedAddresses_complement': str(selectedAddresses_complement),
            'DIM_SHIPPING_DATA_selectedAddresses_neighborhood': str(selectedAddresses_neighborhood),
            'DIM_SHIPPING_DATA_selectedAddresses_postalCode': selectedAddresses_postalCode,
            'DIM_SHIPPING_DATA_selectedAddresses_city': str(selectedAddresses_city),
            'DIM_SHIPPING_DATA_selectedAddresses_state': str(selectedAddresses_state),
            'DIM_SHIPPING_DATA_selectedAddresses_country': str(selectedAddresses_country),
            'DIM_SHIPPING_DATA_selectedAddresses_reference': str(selectedAddresses_reference),
            'transactions_isActive': transactions_isActive,
            'transactions_transactionId': str(transactions_transactionId),
            'transactions_merchantName': str(transactions_merchantName),
            'payments_id': str(payments_id),
            'payments_paymentSystem': payments_paymentSystem,
            'payments_paymentSystemName': str(payments_paymentSystemName),
            'payments_value': payments_value,
            'payments_installments': payments_installments,
            'payments_referenceValue': payments_referenceValue,
            'payments_cardHolder': str(payments_cardHolder),
            'payments_firstDigits': payments_firstDigits,
            'payments_lastDigits': payments_lastDigits,
            'payments_url': str(payments_url),
            'payments_giftCardId': str(payments_giftCardId),
            'payments_giftCardName': str(payments_giftCardName),
            'payments_giftCardCaption': str(payments_giftCardCaption),
            'payments_redemptionCode': str(payments_redemptionCode),
            'payments_group': str(payments_group),
            'payments_dueDate': str(payments_dueDate),
            'payments_cardNumber': str(payments_cardNumber),
            'payments_cvv2': str(payments_cvv2),
            'payments_expireMonth': str(payments_expireMonth),
            'payments_expireYear': str(payments_expireYear),
            'payments_giftCardProvider': str(payments_giftCardProvider),
            'payments_giftCardAsDiscount': str(payments_giftCardAsDiscount),
            'payments_koinUrl': str(payments_koinUrl),
            'payments_accountId': str(payments_accountId),
            'payments_parentAccountId': str(payments_parentAccountId),
            'payments_bankIssuedInvoiceIdentificationNumber': str(payments_bankIssuedInvoiceIdentificationNumber),
            'payments_bankIssuedInvoiceIdentificationNumberFormatted': str(payments_bankIssuedInvoiceIdentificationNumberFormatted),
            'payments_bankIssuedInvoiceBarCodeNumber': str(payments_bankIssuedInvoiceBarCodeNumber),
            'payments_bankIssuedInvoiceBarCodeType': str(payments_bankIssuedInvoiceBarCodeType),
            'payments_Tid': str(payments_Tid),
            'payments_ReturnCode': payments_ReturnCode,
            'payments_Message': str(payments_Message),
            'payments_authId': str(payments_authId),
            'payments_acquirer': str(payments_acquirer),
            'billingAddress_postalCode': str(billingAddress_postalCode),
            'billingAddress_city': str(billingAddress_city),
            'billingAddress_state': str(billingAddress_state),
            'billingAddress_country': str(billingAddress_country),
            'billingAddress_street': str(billingAddress_street),
            'billingAddress_number': billingAddress_number,
            'billingAddress_neighborhood': str(billingAddress_neighborhood),
            'billingAddress_complement': str(billingAddress_complement),
            'billingAddress_reference': str(billingAddress_reference),
            'seller_id': str(seller_id),
            'seller_name': str(seller_name),
            'seller_logo': str(seller_logo),
            'changesAttachment_id': str(changesAttachment_id),
            'storePreferencesData_countryCode': str(storePreferencesData_countryCode),
            'storePreferencesData_currencyCode': str(storePreferencesData_currencyCode),
            'storePreferencesData_currencyLocale': storePreferencesData_currencyLocale,
            'storePreferencesData_currencySymbol': str(storePreferencesData_currencySymbol),
            'storePreferencesData_timeZone': str(storePreferencesData_timeZone),
            'CurrencyDecimalDigits': CurrencyDecimalDigits,
            'CurrencyDecimalSeparator': str(CurrencyDecimalSeparator),
            'CurrencyGroupSeparator': str(CurrencyGroupSeparator),
            'CurrencyGroupSize': CurrencyGroupSize,
            'StartsWithCurrencySymbol': StartsWithCurrencySymbol,
            'marketplace_baseURL': str(baseURL),
            'marketplace_isCertified': str(isCertified),
            'marketplace_name': str(name),
            'itemMetadata_Id': itemMetadata_Id,
            'itemMetadata_Seller': str(itemMetadata_Seller),
            'itemMetadata_Name': str(itemMetadata_Name),
            'itemMetadata_SkuName': str(itemMetadata_SkuName),
            'itemMetadata_ProductId': itemMetadata_ProductId,
            'itemMetadata_RefId': itemMetadata_RefId,
            'itemMetadata_Ean': itemMetadata_Ean,
            'itemMetadata_ImageUrl': str(itemMetadata_ImageUrl),
            'itemMetadata_DetailUrl': str(itemMetadata_DetailUrl),
            'subscriptionData': str(subscriptionData),
            'taxData': str(taxData),
            'courier': str(courier),
            'invoiceNumber': str(invoiceNumber),
            'invoiceValue': str(invoiceValue),
            'invoiceUrl': str(invoiceUrl),
            'issuanceDate': issuanceDate,
            'trackingNumber': str(trackingNumber),
            'invoiceKey': str(invoiceKey),
            'trackingUrl': str(trackingUrl),
            'embeddedInvoice': embeddedInvoice,
            'type': str(type),
            'cfop': str(cfop),
            'restitutions': str(restitutions),
            'volumes': str(volumes),
            'EnableInferItems': str(EnableInferItems),
            'invoice_address': str(invoice_address),
            'userPaymentInfo': str(userPaymentInfo),
            'serialNumbers':str(item_serialNumbers),
            'isActive':isActive,
            'transactionId':str(transactionId),
            'merchantName':str(merchantName),
            'RequestedByUser':str(RequestedByUser),
            'RequestedBySystem':str(RequestedBySystem),
            'RequestedBySellerNotification':str(RequestedBySellerNotification),
            'RequestedByPaymentNotification':str(RequestedByPaymentNotification),
            'Reason':str(Reason),
            'CancellationDate':str(CancellationDate),
            'invoicedDate': str(invoicedDate)}, index=[0])
        init.df = df.append(df1)
        print(df1)
    #except:
    #    cache = 2

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
    print(df)
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
    #job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.autodetect = True
    #job_config.schema = format_schema(table_schema)
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