#!/usr/bin/python
# -*- coding: latin-1 -*-
import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone

naive_dt = datetime.now()
aware_dt = naive_dt.astimezone()
# correct, ISO-8601 (but not UTC)
aware_dt.isoformat(timespec="seconds")
# lets get the time in UTC
utc_dt = aware_dt.astimezone(timezone.utc)
# correct, ISO-8601 and UTC (but not in UTC format)
date_str = utc_dt.isoformat(timespec='milliseconds')
date = date_str.replace("+00:00", "Z")

now = datetime.now()
format = now.strftime('%Y-%m-%d')


def cl_client():
	url = "https://mercury.vtexcommercestable.com.br/api/dataentities/CL/search"
	querystring = {"_fields":"id,beneficio,beneficio2,document,crearGiftcard,profilePicture,proteccionDatos,terminosCondiciones,terminosPago,isCorporate,tradeName,rclastcart,rclastcartvalue,rclastsession,rclastsessiondate,homePhone,phone,brandPurchasedTag,brandVisitedTag,categoryPurchasedTag,categoryVisitedTag,departmentVisitedTag,productPurchasedTag,productVisitedTag,stateRegistration,email,userId,firstName,lastName,document,isNewsletterOptIn,localeDefault,attach,approved,birthDate,businessPhone,carttag,checkouttag,corporateDocument,corporateName,documentType,gender,visitedProductWithStockOutSkusTag,customerClass,priceTables,birthDateMonth,accountId,accountName,dataEntityId,createdBy,createdIn,updatedBy,updatedIn,lastInteractionBy,lastInteractionIn,followers,tags,auto_filter","_where":"createdIn="+format+""}
	headers = {
		"Content-Type": "application/json",
		"Accept": "application/vnd.vtex.ds.v10+json",
		"REST-Range": "resources=0-1000",
		"X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
		"X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"
	}
	response = requests.request("GET", url, headers=headers, params=querystring)
	Fjson = json.loads(response.text)
	return Fjson

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema


df = pd.DataFrame(cl_client(),
columns=['id','beneficio', 'beneficio2', 'document','crearGiftcard','profilePicture','proteccionDatos','terminosCondiciones','terminosPago','isCorporate','tradeName','rclastcart','rclastcartvalue','rclastsession','rclastsessiondate','homePhone','phone','brandPurchasedTag','brandVisitedTag','categoryPurchasedTag','categoryVisitedTag','departmentVisitedTag','productPurchasedTag','productVisitedTag','stateRegistration','email','userId','firstName','lastName','document','isNewsletterOptIn','localeDefault','attach','approved','birthDate','businessPhone','carttag','checkouttag','corporateDocument','corporateName','documentType','gender','visitedProductWithStockOutSkusTag','customerClass','priceTables','birthDateMonth','accountId','accountName','dataEntityId','createdBy','createdIn','updatedBy','updatedIn','lastInteractionBy','lastInteractionIn','followers','tags','auto_filter'])

json_data = df.to_json(orient="split")
json_object = json.loads(json_data)

print(df)
'''
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"/home/bred_valenzuela/full_vtex/vtex/entity/CL/key.json"

table_schema = {
    "name": "beneficio",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "beneficio2",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "crearGiftcard",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "profilePicture",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "proteccionDatos",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "terminosCondiciones",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "terminosPago",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "isCorporate",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "tradeName",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "rclastcart",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "rclastcartvalue",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "rclastsession",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "rclastsessiondate",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "homePhone",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "phone",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "brandPurchasedTag",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "brandVisitedTag",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "categoryPurchasedTag",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "categoryVisitedTag",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "departmentVisitedTag",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "productPurchasedTag",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "productVisitedTag",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "stateRegistration",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "email",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "userId",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "firstName",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "lastName",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "document",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "isNewsletterOptIn",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "localeDefault",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "attach",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "approved",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "birthDate",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "businessPhone",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "carttag",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "checkouttag",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "corporateDocument",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "corporateName",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "documentType",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "gender",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "visitedProductWithStockOutSkusTag",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "customerClass",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "priceTables",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "birthDateMonth",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "id",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "accountId",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "accountName",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "dataEntityId",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "createdBy",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "createdIn",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "updatedBy",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "updatedIn",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "lastInteractionBy",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "lastInteractionIn",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "followers",
    "type": "STRING",
    "mode": "REPEATED"
  },{
    "name": "tags",
    "type": "STRING",
    "mode": "REPEATED"
  },{
    "name": "auto_filter",
    "type": "STRING",
    "mode": "NULLABLE"
  }


project_id = '999847639598'
dataset_id = 'landing_zone'
table_id = 'shopstar_vtex_client'

client  = bigquery.Client(project = project_id)
dataset  = client.dataset(dataset_id)
table = dataset.table(table_id)

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.schema = format_schema(table_schema)
job = client.load_table_from_json(json_object, table, job_config = job_config)
print(job.result())
'''