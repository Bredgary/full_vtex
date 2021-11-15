import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone

def current_cart():
    url = "https://mercury.vtexcommercestable.com.br/api/checkout/pub/orderForm/"
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    response = requests.request("GET", url, headers=headers)
    FJson = json.loads(response.text)
    return FJson

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def run():
    FJson = current_cart()
    df = pd.DataFrame({
        'orderFormId': FJson["orderFormId"], 
        'salesChannel': FJson["salesChannel"],
        'loggedIn': FJson["loggedIn"],
        'isCheckedIn': FJson["isCheckedIn"],
        'storeId': FJson["storeId"],
        'checkedInPickupPointId': FJson["checkedInPickupPointId"],
        'allowManualPrice': FJson["allowManualPrice"],
        'canEditData': FJson["canEditData"],
        'userProfileId': FJson["userProfileId"],
        'userType': FJson["userType"],
        'value': FJson["value"]}, index=[0])

    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
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
    
run()