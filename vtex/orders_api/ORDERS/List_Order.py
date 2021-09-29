import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict


day = datetime.today().strftime('%d')
mouth = datetime.today().strftime('%m')
year = datetime.today().strftime('%y')
dayFrom = int(day) - 27
dayTo = int(day) - 26
limite = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30]
formatoJson = {}
formatoList = []
listDetails = []
list_order = []
order = {}
count = 0

def replace_blank_dict(d):
    if not d:
        return None
    if type(d) is list:
        for list_item in d:
            if type(list_item) is dict:
                for k, v in list_item.items():
                    list_item[k] = replace_blank_dict(v)
    if type(d) is dict:
        for k, v in d.items():
            d[k] = replace_blank_dict(v)
    return d

def get_order(ids):
    url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(ids)+""
    headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    response = requests.request("GET", url, headers=headers)
    formatoJ = json.loads(response.text)
    for k, v in formatoJ.items():
        order[k] = replace_blank_dict(v)
    listDetails.append(formatoJ)

def get_list(pag):
    url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/?page="+str(pag)+""
    querystring = {"f_creationDate":"creationDate:[20"+str(year)+"-"+str(mouth)+"-"+str(dayFrom)+"T02:00:00.000Z TO 20"+str(year)+"-"+str(mouth)+"-"+str(dayTo)+"T01:59:59.999Z]","f_hasInputInvoice":"false"}
    headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    formatoJson = json.loads(response.text)
    lista = formatoJson["list"]
    for i in lista:
        get_order(i["orderId"])
    return formatoJson

for i in limite:
    count = count + 1
    print(str(count)+" Pagina recorrida")
    x = get_list(i)
    if bool(x["list"]):
        list_order.append(x["list"])
    else:
        break

string = json.dumps(listDetails)
characters = "@"
string = ''.join( x for x in string if x not in characters)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/temp.json", "w")
text_file.write(string)
text_file.close()

#system("./convert.py < temp.json > order.json")
system("cat temp.json | jq -c '.[]' > order.json")


print("Cargando a BigQuery order Fecha: 20"+str(year)+"-"+str(mouth)+"-"+str(dayFrom)+" al 20"+str(year)+"-"+str(mouth)+"-"+str(dayTo)+"")

client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/order.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_order'
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
job_config.schema = [
	bigquery.SchemaField("emailTracked", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("approvedBy", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("cancelledBy", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("cancelReason", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("orderId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("sequence", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("marketplaceOrderId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("marketplaceServicesEndpoint", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("sellerOrderId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("origin", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("affiliateId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("salesChannel", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("merchantName", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("statusDescription", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("value", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("creationDate", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("lastChange", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("orderGroup", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("totals", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("totals[].id", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("totals[].name", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("totals[].value", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].uniqueId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].id", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].productId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].ean", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].lockId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].itemAttachment", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].itemAttachment.content", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].itemAttachment.name", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].attachments", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].quantity", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].seller", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].name", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].refId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].price", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].listPrice", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].manualPrice", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].priceTags", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].imageUrl", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].detailUrl", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].components", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].bundleItems", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].params", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].offerings", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].sellerSku", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].priceValidUntil", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].commission", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].tax", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].preSaleDate", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].additionalInfo", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].additionalInfo.brandName", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].additionalInfo.brandId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].additionalInfo.categoriesIds", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].additionalInfo.productClusterId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].additionalInfo.commercialConditionId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].additionalInfo.dimension", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].additionalInfo.offeringInfo", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].additionalInfo.offeringType", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].additionalInfo.offeringTypeId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].measurementUnit", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].unitMultiplier", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].sellingPrice", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].isGift", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].shippingPrice", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].rewardValue", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].freightCommission", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].priceDefinitions", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].taxCode", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].parentItemIndex", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("items[].parentAssemblyBinding", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("marketplaceItems", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.id", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.email", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.firstName", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.lastName", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.documentType", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.document", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.phone", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.corporateName", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.tradeName", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.corporateDocument", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.stateInscription", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.corporatePhone", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.isCorporate", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.userProfileId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("clientProfileData.customerClass", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("giftRegistryData", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("marketingData", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("ratesAndBenefitsData", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("ratesAndBenefitsData.id", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("ratesAndBenefitsData.rateAndBenefitsIdentifiers", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.id", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.addressType", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.receiverName", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.addressId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.postalCode", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.city", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.state", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.country", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.street", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.number", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.neighborhood", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.complement", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.reference", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.address.geoCoordinates", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].itemIndex", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].selectedSla", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].lockTTL", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].price", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].listPrice", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].sellingPrice", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].deliveryWindow", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].deliveryCompany", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].shippingEstimate", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].shippingEstimateDate", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].slas", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].shipsTo", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].deliveryIds", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].deliveryChannel", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].pickupStoreInfo", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].addressId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.logisticsInfo[].polygonName", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.trackingHints", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].addressId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].addressType", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].receiverName", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].street", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].number", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].complement", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].neighborhood", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].postalCode", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].city", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].state", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].country", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].reference", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("shippingData.selectedAddresses[].geoCoordinates", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("paymentData", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("paymentData.transactions", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("paymentData.transactions[].isActive", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("paymentData.transactions[].transactionId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("paymentData.transactions[].merchantName", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("paymentData.transactions[].payments", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("packageAttachment", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("packageAttachment.packages", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("sellers", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("sellers[].id", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("sellers[].name", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("sellers[].logo", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("callCenterOperatorData", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("followUpEmail", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("lastMessage", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("hostname", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("invoiceData", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("Information pertinent to the order's invoice.", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("changesAttachment", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("changesAttachment.id", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("changesAttachment.changesData", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("changesAttachment.changesData[].reason", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("changesAttachment.changesData[].discountValue", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("changesAttachment.changesData[].incrementValue", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("changesAttachment.changesData[].itemsAdded", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("changesAttachment.changesData[].itemsRemoved", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("changesAttachment.changesData[].receipt", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("openTextField", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("roundingError", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("orderFormId", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("commercialConditionData", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("isCompleted", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("customData", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("storePreferencesData", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("storePreferencesData.countryCode", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("storePreferencesData.currencyCode", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("storePreferencesData.currencyFormatInfo", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("storePreferencesData.currencyFormatInfo.CurrencyDecimalDigits", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("storePreferencesData.currencyFormatInfo.CurrencyDecimalSeparator", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("storePreferencesData.currencyFormatInfo.CurrencyGroupSeparator", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("storePreferencesData.currencyFormatInfo.CurrencyGroupSize", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("storePreferencesData.currencyFormatInfo.StartsWithCurrencySymbol", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("storePreferencesData.currencyLocale", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("storePreferencesData.currencySymbol", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("storePreferencesData.timeZone", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("allowCancellation", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("allowEdition", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("isCheckedIn", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("marketplace", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("marketplace.baseURL", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("marketplace.isCertified", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("marketplace.name", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("authorizedDate", "STRING", mode="REQUIRED"),
	bigquery.SchemaField("invoicedDate", "STRING", mode="REQUIRED"),
]
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
with open(filename, "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location="southamerica-east1",  # Must match the destination dataset location.
    job_config=job_config,)  # API request
job.result()  # Waits for table load to complete.
print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
system("rm order.json")
system("rm temp.json")
'''
string = json.dumps(list_order)
text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/order_list.json", "w")
text_file.write(string)
text_file.close()
system("cat order_list.json | jq -c '.[]' > temp.json")
system("cat temp.json | jq -c '.[]' > tabla_order_list.json")


print("Cargando a BigQuery list order Fecha: 20"+str(year)+"-"+str(mouth)+"-"+str(dayFrom)+" al 20"+str(year)+"-"+str(mouth)+"-"+str(dayTo)+"")
client = bigquery.Client()
filename = '/home/bred_valenzuela/full_vtex/vtex/orders_api/ORDERS/tabla_order_list.json'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_list_order'
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.autodetect = True
with open(filename, "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location="southamerica-east1",  # Must match the destination dataset location.
    job_config=job_config,)  # API request
job.result()  # Waits for table load to complete.
print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))

system("rm order_list.json")
system("rm temp.json")
system("rm tabla_order_list.json")

'''