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
        headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
        response = requests.request("GET", url, headers=headers)
        formatoJ = json.loads(response.text)
        return formatoJ["email"]
    except:
        print("No se pudo desencriptar Email: "+str(email))
     
def get_order(id,reg):
   # try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=headers)
        Fjson = json.loads(response.text)
        
        
        
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
                total_id_items = items["id"]
                total_name_items = items["name"]
                total_value_items = items["value"]
        except:
            print("No hay datos items")
        try:
            if Total[1]:
                discounts = Total[1]
                total_id_discounts = discounts["id"]
                total_name_discounts = discounts["name"]
                total_value_discounts = discounts["value"]
        except:
            print("No hay datos discounts")
        try:
            if Total[2]:
                shipping = Total[2]
                total_id_shipping = shipping["id"]
                total_name_shipping = shipping["name"]
                total_value_shipping = shipping["value"]
        except:
            print("No hay datos shipping")
        try:
            if Total[3]:
                tax = Total[3]
                total_id_tax = tax["id"]
                total_name_tax = tax["name"]
                total_value_tax = tax["value"]
        except:
            print("No hay datos tax")
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
            print("No hay datos dim Items")
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
            print("No hay datos. additionalInfo")
        try:
            cubicweight = dimension["cubicweight"]
            height = dimension["height"]
            length = dimension["length"]
            weight = dimension["weight"]
            width = dimension["width"]
        except:
            print("No hay datos. dimension")
        try:
            item_itemAttachment_name = itemAttachment["name"]
        except:
            print("No hay datos. itemAttachment")   
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
            print("No se pudo cargar Client Profile") 
        try:
            id_ratesAndBenefits = ratesAndBenefitsData["id"]
        except:
            print("No se pudo cargar ratesAndBenefitsData")
        try:
            shippingData_id = shippingData["id"]
        except:
            print("No se pudo cargar shippingData")
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
            print("No se pudo cargar address")
            
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
            print("No se pudo cargar address")
        try:
            slas_id = slas_0["id"]
            slas_name = slas_0["name"]
            slas_shippingEstimate = slas_0["shippingEstimate"]
            slas_deliveryWindow = slas_0["deliveryWindow"]
            slas_price = slas_0["price"]
            slas_deliveryChannel = slas_0["deliveryChannel"]
            slas_polygonName = slas_0["polygonName"]
        except:
            print("No hay datos slas")
            
        try:
            slas_pickupStoreInfo_additionalInfo = pickupStoreInfo["additionalInfo"]
            slas_pickupStoreInfo_address = pickupStoreInfo["address"]
            slas_pickupStoreInfo_dockId = pickupStoreInfo["dockId"]
            slas_pickupStoreInfo_friendlyName = pickupStoreInfo["friendlyName"]
            slas_pickupStoreInfo_isPickupStore = pickupStoreInfo["isPickupStore"]
        except:
            print("No hay datos pickupStoreInfo")
        try:
            slas_id_01 = slas_1["id"]
            slas_name_01 = slas_1["name"]
            slas_shippingEstimate_01 = slas_1["shippingEstimate"]
            slas_deliveryWindow_01 = slas_1["deliveryWindow"]
            slas_price_01 = slas_1["price"]
            slas_deliveryChannel_01 = slas_1["deliveryChannel"]
            slas_polygonName_01 = slas_1["polygonName"]
        except:
            print("No hay datos slas_1")
        try:
            slas_pickupStoreInfo_additionalInfo_01 = pickupStoreInfo_1["additionalInfo"]
            slas_pickupStoreInfo_address_01 = pickupStoreInfo_1["address"]
            slas_pickupStoreInfo_dockId_01 = pickupStoreInfo_1["dockId"]
            slas_pickupStoreInfo_friendlyName_01 = pickupStoreInfo_1["friendlyName"]
            slas_pickupStoreInfo_isPickupStore_01 = pickupStoreInfo_1["isPickupStore"]
        except:
            print("No hay datos pickupStoreInfo_1")
        
        try:
            slas_id_02 = slas_2["id"]
            slas_name_02 = slas_2["name"]
            slas_shippingEstimate_02 = slas_2["shippingEstimate"]
            slas_deliveryWindow_02 = slas_2["deliveryWindow"]
            slas_price_02 = slas_2["price"]
            slas_deliveryChannel_02 = slas_2["deliveryChannel"]
            slas_polygonName_02 = slas_2["polygonName"]
        except:
            print("No hay datos slas_2")
        try:
            slas_pickupStoreInfo_additionalInfo_02 = pickupStoreInfo_2["additionalInfo"]
            slas_pickupStoreInfo_address_02 = pickupStoreInfo_2["address"]
            slas_pickupStoreInfo_dockId_02 = pickupStoreInfo_2["dockId"]
            slas_pickupStoreInfo_friendlyName_02 = pickupStoreInfo_2["friendlyName"]
            slas_pickupStoreInfo_isPickupStore_02 = pickupStoreInfo_2["isPickupStore"]
        except:
            print("No hay datos pickupStoreInfo_2")
            
        try:
            slas_id_03 = slas_3["id"]
            slas_name_03 = slas_3["name"]
            slas_shippingEstimate_03 = slas_3["shippingEstimate"]
            slas_deliveryWindow_03 = slas_3["deliveryWindow"]
            slas_price_03 = slas_3["price"]
            slas_deliveryChannel_03 = slas_3["deliveryChannel"]
            slas_polygonName_03 = slas_3["polygonName"]
        except:
            print("No hay datos slas_3")
        try: 
            slas_pickupStoreInfo_additionalInfo_03 = pickupStoreInfo_3["additionalInfo"]
            slas_pickupStoreInfo_address_03 = pickupStoreInfo_3["address"]
            slas_pickupStoreInfo_dockId_03 = pickupStoreInfo_3["dockId"]
            slas_pickupStoreInfo_friendlyName_03 = pickupStoreInfo_3["friendlyName"]
            slas_pickupStoreInfo_isPickupStore_03 = pickupStoreInfo_3["isPickupStore"]
        except:
            print("No hay datos pickupStoreInfo_3")
        try:
            courierId = deliveryIds["courierId"]
            courierName = deliveryIds["courierName"]
            dockId = deliveryIds["dockId"]
            quantity = deliveryIds["quantity"]
            warehouseId = deliveryIds["warehouseId"]
        except:
            print("No hay datos deliveryIds")
        try: 
            pickupStoreInfo_additionalInfo = pickupStoreInfo["additionalInfo"]
            pickupStoreInfo_address = pickupStoreInfo["address"]
            pickupStoreInfo_dockId = pickupStoreInfo["dockId"]
            pickupStoreInfo_friendlyName = pickupStoreInfo["friendlyName"]
            pickupStoreInfo_isPickupStore = pickupStoreInfo["isPickupStore"]
        except:
            print("No hay datos pickupStoreInfo")
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
            print("No hay datos selectedAddresses")
        try:
            transactions_isActive = transactions["isActive"]
            transactions_transactionId = transactions["transactionId"]
            transactions_merchantName = transactions["merchantName"]
        except:
            print("No hay datos transactions")
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
            print("No hay datos payments")
        try:
            connectorResponses = payments["connectorResponses"]
            payments_ReturnCode = connectorResponses["ReturnCode"]
            payments_Message = connectorResponses["Message"]
            payments_authId = connectorResponses["authId"]
            payments_acquirer = connectorResponses["acquirer"]
        except:
            print("No hay datos connectorResponses")
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
            print("No hay datos billingAddress")
        try:
            seller_id = sellers["id"]
            seller_name = sellers["name"]
            seller_logo = sellers["logo"]
        except:
            print("No hay datos seller")
        try:
            changesAttachment_id = Fjson["changesAttachment"]
        except:
            print("No hay datos changesAttachment")
        try:
            storePreferencesData_countryCode = storePreferencesData["countryCode"]
            storePreferencesData_currencyCode = storePreferencesData["currencyCode"]
            storePreferencesData_currencyLocale = storePreferencesData["currencyLocale"]
            storePreferencesData_currencySymbol = storePreferencesData["currencySymbol"]
            storePreferencesData_timeZone = storePreferencesData["timeZone"]
        except:
            print("No hay datos storePreferencesData")
        try:
            CurrencyDecimalDigits = currencyFormatInfo["CurrencyDecimalDigits"]
            CurrencyDecimalSeparator = currencyFormatInfo["CurrencyDecimalSeparator"]
            CurrencyGroupSeparator = currencyFormatInfo["CurrencyGroupSeparator"]
            CurrencyGroupSize = currencyFormatInfo["CurrencyGroupSize"]
            StartsWithCurrencySymbol = currencyFormatInfo["StartsWithCurrencySymbol"]
        except:
            print("No hay datos currencyFormatInfo")
        try:
            baseURL = marketplace["baseURL"]
            isCertified = marketplace["isCertified"]
            name = marketplace["name"]
        except:
            print("No hay datos marketplace")
        
        try:
            client_email = decrypt_email(str(client_email))
        except:
            client_email = None
            print("nulo")
        
        
        
        
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
            print("vacio")
        
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
            print("vacio")
            
    
        try:
            dim_invoiceData = Fjson["invoiceData"]
            invoice_address = dim_invoiceData["address"]
            userPaymentInfo = dim_invoiceData["userPaymentInfo"]
        except:
            print("vacio")
        
        try:    
            isActive = transactions["isActive"]
            transactionId = transactions["transactionId"]
            merchantName = transactions["merchantName"]
        except:
            print("vacio")
            
        try:
            cancellationData = Fjson["cancellationData"]
            RequestedByUser = cancellationData["RequestedByUser"]
            RequestedBySystem = cancellationData["RequestedBySystem"]
            RequestedBySellerNotification = cancellationData["RequestedBySellerNotification"]
            RequestedByPaymentNotification = cancellationData["RequestedByPaymentNotification"]
            Reason = cancellationData["Reason"]
        except:
            print("cancellationData")
    
        df1 = pd.DataFrame({
            'orderId': id,
            'emailTracked': emailTracked,
            'approvedBy': approvedBy,
            'cancelledBy': cancelledBy,
            'cancelReason': cancelReason,
            'sequence': sequence,
            'marketplaceOrderId': marketplaceOrderId,
            'marketplaceServicesEndpoint': marketplaceServicesEndpoint,
            'sellerOrderId': sellerOrderId,
            'origin': origin,
            'affiliateId': affiliateId,
            'salesChannel': salesChannel,
            'merchantName': merchantName,
            'status': status,
            'statusDescription': statusDescription,
            'value': value,
            'creationDate': creationDate,
            'lastChange': lastChange,
            'orderGroup': orderGroup,
            'giftRegistryData': giftRegistryData,
            'callCenterOperatorData': callCenterOperatorData,
            'followUpEmail': followUpEmail,
            'lastMessage': lastMessage,
            'hostname': hostname,
            'openTextField': openTextField,
            'roundingError': roundingError,
            'orderFormId': orderFormId,
            'commercialConditionData': commercialConditionData,
            'isCompleted': isCompleted,
            'customData': customData,
            'allowCancellation': allowCancellation,
            'allowEdition': allowEdition,
            'isCheckedIn': isCheckedIn,
            'authorizedDate': authorizedDate,
            'DIM_TOTAL_id_items': total_id_items,
            'DIM_TOTAL_name_items': total_name_items,
            'DIM_TOTAL_value_items': total_value_items,
            'DIM_TOTAL_id_discounts': total_id_discounts,
            'DIM_TOTAL_name_discounts': total_name_discounts,
            'DIM_TOTAL_value_discounts': total_value_discounts,
            'DIM_TOTAL_id_shipping': total_id_shipping,
            'DIM_TOTAL_name_shipping': total_name_shipping,
            'DIM_TOTAL_value_shipping': total_value_shipping,
            'DIM_TOTAL_id_tax': total_id_tax,
            'DIM_TOTAL_name_tax': total_name_tax,
            'DIM_TOTAL_value_tax': total_value_tax,
            'DIM_TOTAL_id_change': total_id_change,
            'DIM_TOTAL_name_change': total_name_change,
            'DIM_TOTAL_value_change': total_value_change,
            'DIM_ITEMS_uniqueId': items_uniqueId,
            'DIM_ITEMS_items_id': items_id,
            'DIM_ITEMS_productId': items_productId,
            'DIM_ITEMS_ean': items_ean,
            'DIM_ITEMS_lockId': items_lockId,
            'DIM_ITEMS_quantity': item_quantity,
            'DIM_ITEMS_seller': item_seller,
            'DIM_ITEMS_name': item_name,
            'DIM_ITEMS_refId': item_refId,
            'DIM_ITEMS_price': item_price,
            'DIM_ITEMS_listPrice': item_listPrice,
            'DIM_ITEMS_manualPrice': item_manualPrice,
            'DIM_ITEMS_imageUrl': item_imageUrl,
            'DIM_ITEMS_detailUrl': item_detailUrl,
            'DIM_ITEMS_sellerSku': item_sellerSku,
            'DIM_ITEMS_priceValidUntil': item_priceValidUntil,
            'DIM_ITEMS_commission': item_commission,
            'DIM_ITEMS_tax': item_tax,
            'DIM_ITEM_preSaleDate': item_preSaleDate,
            'DIM_ITEM_measurementUnit': item_measurementUnit,
            'DIM_ITEM_unitMultiplier': item_unitMultiplier,
            'DIM_ITEM_sellingPrice': item_sellingPrice,
            'DIM_ITEM_isGift': item_isGift,
            'DIM_ITEM_shippingPrice': item_shippingPrice,
            'DIM_ITEM_rewardValue': item_rewardValue,
            'DIM_ITEM_freightCommission': item_freightCommission,
            'DIM_ITEM_priceDefinition': item_price_definition,
            'DIM_ITEM_taxCode': item_taxCode,
            'DIM_ITEM_parentItemIndex': item_parentItemIndex,
            'DIM_ITEM_parentAssemblyBinding': item_parentAssemblyBinding,
            'DIM_ITEM_itemAttachment_name': item_itemAttachment_name,
            'DIM_ITEM_AInfo_brandName': brandName,
            'DIM_ITEM_AInfo_brandId': brandId,
            'DIM_ITEM_AInfo_categoriesIds': categoriesIds,
            'DIM_ITEM_AInfo_productClusterId': productClusterId,
            'DIM_ITEM_AInfo_commercialConditionId': commercialConditionId,
            'DIM_ITEM_AInfo_offeringInfo': offeringInfo,
            'DIM_ITEM_AInfo_offeringType': offeringType,
            'DIM_ITEM_AInfo_offeringTypeId': offeringTypeId,
            'DIM_ITEM_AInfo_cubicweight': cubicweight,
            'DIM_ITEM_AInfo_dim_height': height,
            'DIM_ITEM_AInfo_dim_length': length,
            'DIM_ITEM_AInfo_dim_weight': weight,
            'DIM_ITEM_AInfo_dim_width': width,
            'DIM_ITEM_calculatedSellingPrice': calculatedSellingPrice,
            'DIM_ITEM_priceDefinition_total': total,
            'DIM_CLIENT': client_id,
            'DIM_CLIENT_email': client_email,
            'DIM_CLIENT_firstName': client_firstName,
            'DIM_CLIENT_lastName': client_lastName,
            'DIM_CLIENT_documentType': client_documentType,
            'DIM_CLIENT_document': client_document,
            'DIM_CLIENT_phone': client_phone,
            'DIM_CLIENT_corporateName': client_corporateName,
            'DIM_CLIENT_tradeName': client_tradeName,
            'DIM_CLIENT_corporateDocument': client_corporateDocument,
            'DIM_CLIENT_stateInscription': client_stateInscription,
            'DIM_CLIENT_corporatePhone': client_corporatePhone,
            'DIM_CLIENT_isCorporate': client_isCorporate,
            'DIM_CLIENT_userProfileId': client_userProfileId,
            'DIM_CLIENT_customerClass': client_customerClass,
            'id_ratesAndBenefits': id_ratesAndBenefits,
            'DIM_SHIPPING_DATA_shippingData_id': shippingData_id,
            'DIM_SHIPPING_DATA_addressType': addressType,
            'DIM_SHIPPING_DATA_receiverName': receiverName,
            'DIM_SHIPPING_DATA_addressId': addressId,
            'DIM_SHIPPING_DATA_postalCode': postalCode,
            'DIM_SHIPPING_DATA_city': city,
            'DIM_SHIPPING_DATA_state': state,
            'DIM_SHIPPING_DATA_country': country,
            'DIM_SHIPPING_DATA_street': street,
            'DIM_SHIPPING_DATA_number': number,
            'DIM_SHIPPING_DATA_neighborhood': neighborhood,
            'DIM_SHIPPING_DATA_complement': complement,
            'DIM_SHIPPING_DATA_reference': reference,
            'DIM_SHIPPING_DATA_deliveryChannel': deliveryChannel,
            'DIM_SHIPPING_DATA_addressId': addressId,
            'DIM_SHIPPING_DATA_polygonName': polygonName,
            'DIM_SHIPPING_DATA_itemIndex': itemIndex,
            'DIM_SHIPPING_DATA_selectedSla': selectedSla,
            'DIM_SHIPPING_DATA_lockTTL': lockTTL,
            'DIM_SHIPPING_DATA_price': price,
            'DIM_SHIPPING_DATA_listPrice': listPrice,
            'DIM_SHIPPING_DATA_sellingPrice': sellingPrice,
            'DIM_SHIPPING_DATA_deliveryCompany': deliveryCompany,
            'DIM_SHIPPING_DATA_shippingEstimate': shippingEstimate,
            'DIM_SHIPPING_DATA_shippingEstimateDate': shippingEstimateDate,
            'DIM_SHIPPING_DATA_slas_id': slas_id,
            'DIM_SHIPPING_DATA_slas_name': slas_name,
            'DIM_SHIPPING_DATA_slas_shippingEstimate': slas_shippingEstimate,
            'DIM_SHIPPING_DATA_slas_price': slas_price,
            'DIM_SHIPPING_DATA_slas_deliveryChannel': slas_deliveryChannel,
            'DIM_SHIPPING_DATA_slas_polygonName': slas_polygonName,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo': slas_pickupStoreInfo_additionalInfo,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId': slas_pickupStoreInfo_dockId,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName': slas_pickupStoreInfo_friendlyName,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore': slas_pickupStoreInfo_isPickupStore,
            'DIM_SHIPPING_DATA_slas_id_01': slas_id_01,
            'DIM_SHIPPING_DATA_slas_name_01': slas_name_01,
            'DIM_SHIPPING_DATA_slas_shippingEstimate_01': slas_shippingEstimate_01,
            'DIM_SHIPPING_DATA_slas_price_01': slas_price_01,
            'DIM_SHIPPING_DATA_slas_deliveryChannel_01': slas_deliveryChannel_01,
            'DIM_SHIPPING_DATA_slas_polygonName_01': slas_polygonName_01,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_01': slas_pickupStoreInfo_additionalInfo_01,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_01': slas_pickupStoreInfo_dockId_01,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_01': slas_pickupStoreInfo_friendlyName_01,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_01': slas_pickupStoreInfo_isPickupStore_01,
            'DIM_SHIPPING_DATA_slas_id_02': slas_id_02,
            'DIM_SHIPPING_DATA_slas_name_02': slas_name_02,
            'DIM_SHIPPING_DATA_slas_shippingEstimate_02': slas_shippingEstimate_02,
            'DIM_SHIPPING_DATA_slas_deliveryWindow_02': slas_deliveryWindow_02,
            'DIM_SHIPPING_DATA_slas_price_02': slas_price_02,
            'DIM_SHIPPING_DATA_slas_deliveryChannel_02': slas_deliveryChannel_02,
            'DIM_SHIPPING_DATA_slas_polygonName_02': slas_polygonName_02,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_02': slas_pickupStoreInfo_additionalInfo_02,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_02': slas_pickupStoreInfo_dockId_02,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_02': slas_pickupStoreInfo_friendlyName_02,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_02': slas_pickupStoreInfo_isPickupStore_02,
            'DIM_SHIPPING_DATA_slas_id_03': slas_id_03,
            'DIM_SHIPPING_DATA_slas_name_03': slas_name_03,
            'DIM_SHIPPING_DATA_slas_shippingEstimate_03': slas_shippingEstimate_03,
            'DIM_SHIPPING_DATA_slas_price_03': slas_price_03,
            'DIM_SHIPPING_DATA_slas_deliveryChannel_03': slas_deliveryChannel_03,
            'DIM_SHIPPING_DATA_slas_polygonName_03': slas_polygonName_03,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_additionalInfo_03': slas_pickupStoreInfo_additionalInfo_03,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_dockId_03': slas_pickupStoreInfo_dockId_03,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_friendlyName_03': slas_pickupStoreInfo_friendlyName_03,
            'DIM_SHIPPING_DATA_slas_pickupStoreInfo_isPickupStore_03': slas_pickupStoreInfo_isPickupStore_03,
            'DIM_SHIPPING_DATA_courierId_delivery': courierId,
            'DIM_SHIPPING_DATA_courierName_delivery': courierName,
            'DIM_SHIPPING_DATA_dockId_delivery': dockId,
            'DIM_SHIPPING_DATA_quantity_delivery': quantity,
            'DIM_SHIPPING_DATA_warehouseId': warehouseId,
            'DIM_SHIPPING_DATA_pickupStoreInfo_additionalInfo': pickupStoreInfo_additionalInfo,
            'DIM_SHIPPING_DATA_pickupStoreInfo_dockId': pickupStoreInfo_dockId,
            'DIM_SHIPPING_DATA_pickupStoreInfo_friendlyName': pickupStoreInfo_friendlyName,
            'DIM_SHIPPING_DATA_pickupStoreInfo_isPickupStore': pickupStoreInfo_isPickupStore,
            'DIM_SHIPPING_DATA_selectedAddresses_addressId': selectedAddresses_addressId,
            'DIM_SHIPPING_DATA_selectedAddresses_addressType': selectedAddresses_addressType,
            'DIM_SHIPPING_DATA_selectedAddresses_receiverName': selectedAddresses_receiverName,
            'DIM_SHIPPING_DATA_selectedAddresses_street': selectedAddresses_street,
            'DIM_SHIPPING_DATA_selectedAddresses_number': selectedAddresses_number,
            'DIM_SHIPPING_DATA_selectedAddresses_complement': selectedAddresses_complement,
            'DIM_SHIPPING_DATA_selectedAddresses_neighborhood': selectedAddresses_neighborhood,
            'DIM_SHIPPING_DATA_selectedAddresses_postalCode': selectedAddresses_postalCode,
            'DIM_SHIPPING_DATA_selectedAddresses_city': selectedAddresses_city,
            'DIM_SHIPPING_DATA_selectedAddresses_state': selectedAddresses_state,
            'DIM_SHIPPING_DATA_selectedAddresses_country': selectedAddresses_country,
            'DIM_SHIPPING_DATA_selectedAddresses_reference': selectedAddresses_reference,
            'transactions_isActive': transactions_isActive,
            'transactions_transactionId': transactions_transactionId,
            'transactions_merchantName': transactions_merchantName,
            'payments_id': payments_id,
            'payments_paymentSystem': payments_paymentSystem,
            'payments_paymentSystemName': payments_paymentSystemName,
            'payments_value': payments_value,
            'payments_installments': payments_installments,
            'payments_referenceValue': payments_referenceValue,
            'payments_cardHolder': payments_cardHolder,
            'payments_firstDigits': payments_firstDigits,
            'payments_lastDigits': payments_lastDigits,
            'payments_url': payments_url,
            'payments_giftCardId': payments_giftCardId,
            'payments_giftCardName': payments_giftCardName,
            'payments_giftCardCaption': payments_giftCardCaption,
            'payments_redemptionCode': payments_redemptionCode,
            'payments_group': payments_group,
            'payments_dueDate': payments_dueDate,
            'payments_cardNumber': payments_cardNumber,
            'payments_cvv2': payments_cvv2,
            'payments_expireMonth': payments_expireMonth,
            'payments_expireYear': payments_expireYear,
            'payments_giftCardProvider': payments_giftCardProvider,
            'payments_giftCardAsDiscount': payments_giftCardAsDiscount,
            'payments_koinUrl': payments_koinUrl,
            'payments_accountId': payments_accountId,
            'payments_parentAccountId': payments_parentAccountId,
            'payments_bankIssuedInvoiceIdentificationNumber': payments_bankIssuedInvoiceIdentificationNumber,
            'payments_bankIssuedInvoiceIdentificationNumberFormatted': payments_bankIssuedInvoiceIdentificationNumberFormatted,
            'payments_bankIssuedInvoiceBarCodeNumber': payments_bankIssuedInvoiceBarCodeNumber,
            'payments_bankIssuedInvoiceBarCodeType': payments_bankIssuedInvoiceBarCodeType,
            'payments_Tid': payments_Tid,
            'payments_ReturnCode': payments_ReturnCode,
            'payments_Message': payments_Message,
            'payments_authId': payments_authId,
            'payments_acquirer': payments_acquirer,
            'billingAddress_postalCode': billingAddress_postalCode,
            'billingAddress_city': billingAddress_city,
            'billingAddress_state': billingAddress_state,
            'billingAddress_country': billingAddress_country,
            'billingAddress_street': billingAddress_street,
            'billingAddress_number': billingAddress_number,
            'billingAddress_neighborhood': billingAddress_neighborhood,
            'billingAddress_complement': billingAddress_complement,
            'billingAddress_reference': billingAddress_reference,
            'seller_id': seller_id,
            'seller_name': seller_name,
            'seller_logo': seller_logo,
            'changesAttachment_id': changesAttachment_id,
            'storePreferencesData_countryCode': storePreferencesData_countryCode,
            'storePreferencesData_currencyCode': storePreferencesData_currencyCode,
            'storePreferencesData_currencyLocale': storePreferencesData_currencyLocale,
            'storePreferencesData_currencySymbol': storePreferencesData_currencySymbol,
            'storePreferencesData_timeZone': storePreferencesData_timeZone,
            'CurrencyDecimalDigits': CurrencyDecimalDigits,
            'CurrencyDecimalSeparator': CurrencyDecimalSeparator,
            'CurrencyGroupSeparator': CurrencyGroupSeparator,
            'CurrencyGroupSize': CurrencyGroupSize,
            'StartsWithCurrencySymbol': StartsWithCurrencySymbol,
            'marketplace_baseURL': baseURL,
            'marketplace_isCertified': isCertified,
            'marketplace_name': name,
            'itemMetadata_Id': itemMetadata_Id,
            'itemMetadata_Seller': itemMetadata_Seller,
            'itemMetadata_Name': itemMetadata_Name,
            'itemMetadata_SkuName': itemMetadata_SkuName,
            'itemMetadata_ProductId': itemMetadata_ProductId,
            'itemMetadata_RefId': itemMetadata_RefId,
            'itemMetadata_Ean': itemMetadata_Ean,
            'itemMetadata_ImageUrl': itemMetadata_ImageUrl,
            'itemMetadata_DetailUrl': itemMetadata_DetailUrl,
            'subscriptionData': subscriptionData,
            'taxData': taxData,
            'courier': courier,
            'invoiceNumber': invoiceNumber,
            'invoiceValue': invoiceValue,
            'invoiceUrl': invoiceUrl,
            'issuanceDate': issuanceDate,
            'trackingNumber': trackingNumber,
            'invoiceKey': invoiceKey,
            'trackingUrl': trackingUrl,
            'embeddedInvoice': embeddedInvoice,
            'type': type,
            'cfop': cfop,
            'restitutions': restitutions,
            'volumes': volumes,
            'EnableInferItems': EnableInferItems,
            'invoice_address': invoice_address,
            'userPaymentInfo': userPaymentInfo,
            'serialNumbers':item_serialNumbers,
            'isActive':isActive,
            'transactionId':transactionId,
            'merchantName':merchantName,
            'RequestedByUser':RequestedByUser,
            'RequestedBySystem':RequestedBySystem,
            'RequestedBySellerNotification':RequestedBySellerNotification,
            'RequestedByPaymentNotification':RequestedByPaymentNotification,
            'Reason':Reason,
            'CancellationDate':CancellationDate,
            'invoicedDate': init.invoicedDate}, index=[0])
        init.df = init.df.append(df1)
        print("Registro: "+str(reg))
    #except:
     #   print("vacio")

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
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()        


def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT DISTINCT orderId  FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`WHERE (orderId NOT IN (SELECT orderId FROM `shopstar-datalake.test.shopstar_vtex_order`))')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order(row.orderId,registro)
        if registro == 5:
            run()
        if registro == 10:
            run()
        if registro == 100:
            run()
        if registro == 200:
            run()
        if registro == 300:
            run()
        if registro == 400:
            run()
        if registro == 500:
            run()
        if registro == 600:
            run()
        if registro == 700:
            run()
        if registro == 800:
            run()
        if registro == 900:
            run()
        if registro == 1000:
            run()
        if registro == 1100:
            run()
        if registro == 1200:
            run()
        if registro == 1300:
            run()
        if registro == 1400:
            run()
        if registro == 1500:
            run()
        if registro == 1600:
            run()
        if registro == 1700:
            run()
        if registro == 1800:
            run()
        if registro == 1900:
            run()
        if registro == 2000:
            run()
        if registro == 2500:
            run()
        if registro == 3000:
            run()
        if registro == 3500:
            run()
        if registro == 4000:
            run()
        if registro == 4500:
            run()
        if registro == 5000:
            run()
        if registro == 5500:
            run()
        if registro == 7000:
            run()
        if registro == 7500:
            run()
        if registro == 8000:
            run()
        if registro == 8500:
            run()
        if registro == 9000:
            run()
        if registro == 9500:
            run()
        if registro == 10000:
            run()
        if registro == 11000:
            run()
        if registro == 12000:
            run()
        if registro == 13000:
            run()
        if registro == 14000:
            run()
        if registro == 15000:
            run()
        if registro == 16000:
            run()
    run()
        
    
get_params()