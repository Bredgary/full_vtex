import requests
import json
import os
import re
import datetime
from datetime import date
from datetime import timedelta
from os import system
from google.cloud import bigquery
from itertools import chain
from collections import defaultdict

class Init:
	today = datetime.date.today()
	yesterday = today - datetime.timedelta(days=1)
	before_yesterday = today - datetime.timedelta(days=2)
	
class Params:
	orderId:str = None
	creationDate:str = None
	clientName:str = None
	totalValue:str = None
	paymentNames:str = None
	status:str = None
	statusDescription:str = None
	marketPlaceOrderId:str = None
	sequence:str = None
	salesChannel:str = None
	affiliateId:str = None
	origin:str = None
	workflowInErrorState:str = None
	workflowInRetry:str = None
	lastMessageUnread:str = None
	ShippingEstimatedDate:str = None
	ShippingEstimatedDateMax:str = None
	ShippingEstimatedDateMin:str = None
	orderIsComplete:str = None
	listId:str = None
	listType:str = None
	authorizedDate:str = None
	callCenterOperatorName:str = None
	totalItems:str = None
	currencyCode:str = None

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def get_order_list(page):
	url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders?page="+str(page)+""
	querystring = {"f_creationDate":"creationDate:["+str(Init.before_yesterday)+"T02:00:00.000Z TO "+str(Init.yesterday)+"T01:59:59.999Z]","f_hasInputInvoice":"false"}
	headers = {"Accept": "application/json","Content-Type": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	FJson = json.loads(response.text)
	return FJson

def paging():
  for x in range(30):
    FJson = get_order_list(x)
    if not FJson["list"]:
      FJson.update(FJson)
    else:
      break
  return FJson

def run():
    FJson = paging()
    df = pd.DataFrame({
		Params.orderId: FJson["orderId"]}, index=[0])

    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    print(json_object)
    '''
    table_schema = {
        "name": "orderFormId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "salesChannel",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "loggedIn",
        "type": "BOOL",
        "mode": "NULLABLE"
    },{
        "name": "isCheckedIn",
        "type": "BOOL",
        "mode": "NULLABLE"
    },{
        "name": "storeId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "checkedInPickupPointId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "allowManualPrice",
        "type": "BOOL",
        "mode": "NULLABLE"
    },{
        "name": "canEditData",
        "type": "BOOL",
        "mode": "NULLABLE"
    },{
        "name": "userProfileId",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "userType",
        "type": "STRING",
        "mode": "NULLABLE"
    },{
        "name": "value",
        "type": "INTEGER",
        "mode": "NULLABLE"}

    project_id = '999847639598'
    dataset_id = 'landing_zone'
    table_id = 'shopstar_vtex_current_cart'

    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = format_schema(table_schema)
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
	'''
run()



