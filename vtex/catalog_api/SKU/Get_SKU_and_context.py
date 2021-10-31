#!/usr/bin/python
# -*- coding: latin-1 -*-
import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone

count = 0
listaIDS = []

def get_sku(id,count):
	try:
		url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyid/"""+str(id)+""
		querystring = {"sc":"1"}
		headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
		response = requests.request("GET", url, headers=headers, params=querystring)
		jsonF = json.loads(response.text)
		listaIDS.append(jsonF)
		print("Get_SKU_and_context.py Terminando: "+str(count))
	except:
		text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/"+str(count)+"_tenp.txt", "w")
		text_file.write(str(count))
		text_file.close()

def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/id_sku.json','r')
	data_from_string = f_01.read()
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		count += 1
		get_sku(i,count)
	print(str(count)+" registro finalizado.")

operacion_fenix(count)

text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/sku_context", "w")
text_file.write(str(listaIDS))
text_file.close()
'''
def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema


df = pd.DataFrame(cl_client(),
columns=['beneficio','beneficio2','crearGiftcard','profilePicture','proteccionDatos','terminosCondiciones','terminosPago','tradeName','rclastcart','rclastsession','rclastsessiondate','homePhone','phone','stateRegistration','email','userId','firstName','lastName','document','localeDefault','attach','approved','birthDate','businessPhone','corporateDocument','corporateName','documentType','gender','customerClass','priceTables','id','accountId','accountName','dataEntityId','createdBy','createdIn','updatedBy','updatedIn','lastInteractionBy','lastInteractionIn','followers','auto_filter'])
df.reset_index(drop=True, inplace=True)

json_data = df.to_json(orient = 'records')
json_object = json.loads(json_data)


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"/home/bred_valenzuela/full_vtex/vtex/entity/CL/key.json"

table_schema = {
    "name": "id",
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
    "name": "localeDefault",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "attach",
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
    "name": "tradeName",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "rclastcart",
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
    "name": "stateRegistration",
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
    "name": "customerClass",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "priceTables",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "beneficio",
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
