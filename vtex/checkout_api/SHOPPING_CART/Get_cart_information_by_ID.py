import pandas as pd
import numpy as np
import os, json
import requests
import re
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict
from datetime import datetime, timezone

idTemporal = "ede846222cd44046ba6c638442c3505a"

def current_cart(id):
	url = "https://mercury.vtexcommercestable.com.br/api/checkout/pub/orderForm/"+str(id)+""
	querystring = {"refreshOutdatedData":"true"}
	headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	result = json.dumps(FJson)
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/checkout_api/SHOPPING_CART/SHOPPING_CART_BY_ID.json", "w")
	text_file.write(result)
	text_file.close()
	cargando_bigquery()
	
def run():
    FJson = current_cart()
    df = pd.DataFrame({
        'subscriptionData': FJson["subscriptionData"], 
        'ratesAndBenefitsData': FJson["ratesAndBenefitsData"],
        'hooksData': FJson["hooksData"],
        'customData': FJson["customData"],
        'invoiceData': FJson["invoiceData"],
        'paymentData': FJson["paymentData"],
		'shippingData': FJson["shippingData"],
		'commercialConditionData': FJson["commercialConditionData"],
		'selectableGifts': FJson["selectableGifts"],
		'clientPreferencesData': FJson["clientPreferencesData"],
		'items': FJson["items"],
		'giftRegistryData': FJson["giftRegistryData"],
		'loggedIn': FJson["loggedIn"],
		'value': FJson["value"],
		'ignoreProfileData': FJson["ignoreProfileData"],
		'userProfileId': FJson["userProfileId"],
		'sellers': FJson["sellers"],
		'isCheckedIn': FJson["isCheckedIn"],
		'userType': FJson["userType"],
		'itemsOrdination': FJson["itemsOrdination"],
		'messages': FJson["messages"],
		'canEditData': FJson["canEditData"],
		'totalizers': FJson["totalizers"],
		'clientProfileData': FJson["clientProfileData"],
		'checkedInPickupPointId': FJson["checkedInPickupPointId"],
		'itemMetadata': FJson["itemMetadata"],
		'allowManualPrice': FJson["allowManualPrice"],
		'salesChannel': FJson["salesChannel"],
		'storeId': FJson["storeId"],
		'openTextField': FJson["openTextField"],
		'marketingData': FJson["marketingData"],
		'storePreferencesData': FJson["storePreferencesData"],
		'orderFormId': FJson["orderFormId"])

    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    table_schema = {
		"name": "subscriptionData",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "ratesAndBenefitsData",
		"type": "STRING",
		"mode": "NULLABLE"
    },{
		"name": "hooksData",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "customData",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "invoiceData",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "paymentData",
		"type": "RECORD",
		"mode": "NULLABLE",
		
	},{
		"name": "shippingData",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "commercialConditionData",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "selectableGifts",
		"type": "STRING",
		"mode": "REPEATED"
	},{
		"name": "clientPreferencesData",
		"type": "RECORD",
		"mode": "NULLABLE",
	},{
		"name": "items",
		"type": "STRING",
		"mode": "REPEATED"
	},{
		"name": "giftRegistryData",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "loggedIn",
		"type": "BOOLEAN",
		"mode": "NULLABLE"
	},{
		"name": "value",
		"type": "INTEGER",
		"mode": "NULLABLE"
	},{
		"name": "ignoreProfileData",
		"type": "BOOLEAN",
		"mode": "NULLABLE"
	},{
		"name": "userProfileId",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "sellers",
		"type": "STRING",
		"mode": "REPEATED"
	},{
		"name": "isCheckedIn",
		"type": "BOOLEAN",
		"mode": "NULLABLE"
	},{
		"name": "userType",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "itemsOrdination",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "messages",
		"type": "STRING",
		"mode": "REPEATED"
	},{
		"name": "canEditData",
		"type": "BOOLEAN",
		"mode": "NULLABLE"
	},{
		"name": "totalizers",
		"type": "STRING",
		"mode": "REPEATED"
	},{
		"name": "clientProfileData",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "checkedInPickupPointId",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "itemMetadata",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "allowManualPrice",
		"type": "BOOLEAN",
		"mode": "NULLABLE"
	},{
		"name": "salesChannel",
		"type": "INTEGER",
		"mode": "NULLABLE"
	},{
		"name": "storeId",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "openTextField",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "marketingData",
		"type": "STRING",
		"mode": "NULLABLE"
	},{
		"name": "storePreferencesData",
		"type": "RECORD",
		"mode": "NULLABLE",
	},{
		"name": "orderFormId",
		"type": "STRING",
		"mode": "NULLABLE"
	}

    project_id = '999847639598'
    dataset_id = 'landing_zone'
    table_id = 'shopstar_vtex_cart_information'

    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = format_schema(table_schema)
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    
run()