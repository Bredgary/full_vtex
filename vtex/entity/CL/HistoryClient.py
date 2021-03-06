import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from datetime import date, timedelta

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

def cl_client(fecha):
	url = "https://mercury.vtexcommercestable.com.br/api/dataentities/CL/search"
	querystring = {"_fields":"isCorporate,beneficio,beneficio2,crearGiftcard,profilePicture,proteccionDatos,terminosCondiciones,terminosPago,tradeName,rclastcart,rclastsession,rclastsessiondate,homePhone,phone,stateRegistration,email,userId,firstName,lastName,document,localeDefault,attach,approved,birthDate,businessPhone,corporateDocument,corporateName,documentType,gender,customerClass,priceTables,id,accountId,accountName,dataEntityId,createdBy,createdIn,updatedBy,updatedIn,lastInteractionBy,lastInteractionIn","_where":"createdIn="+fecha+""}
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

def delete_duplicate():
	try:
		print("Eliminando duplicados")
		client = bigquery.Client()
		QUERY = (
			'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_client` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_client`')
		query_job = client.query(QUERY)
		rows = query_job.result()
		print(rows)
	except:
		print("Consulta SQL no ejecutada")

def run(fecha):
	df = pd.DataFrame(cl_client(fecha),
					columns=['beneficio','beneficio2','crearGiftcard','profilePicture','proteccionDatos','terminosCondiciones','terminosPago','tradeName','rclastcart','rclastsession','rclastsessiondate','homePhone','phone','stateRegistration','email','userId','firstName','lastName','document','localeDefault','attach','approved','birthDate','businessPhone','corporateDocument','corporateName','documentType','gender','customerClass','priceTables','id','accountId','accountName','dataEntityId','createdBy','createdIn','updatedBy','updatedIn','lastInteractionBy','lastInteractionIn'])
	df.reset_index(drop=True, inplace=True)

	json_data = df.to_json(orient = 'records')
	json_object = json.loads(json_data)
	
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
	    "type": "DATE",
	    "mode": "NULLABLE"
	  },{
	    "name": "updatedBy",
	    "type": "DATE",
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
	    "type": "DATE",
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
	    "type": "DATE",
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
	    "type": "DATE",
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
	    "name": "isCorporate",
	    "type": "STRING",
	    "mode": "NULLABLE"
	  }

	project_id = '999847639598'
	dataset_id = 'test'
	table_id = 'shopstar_vtex_client'
	
	client  = bigquery.Client(project = project_id)
	dataset  = client.dataset(dataset_id)
	table = dataset.table(table_id)
	
	job_config = bigquery.LoadJobConfig()
	job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
	job_config.schema = format_schema(table_schema)
	#job_config.autodetect = True
	job = client.load_table_from_json(json_object, table, job_config = job_config)
	print(job.result())
	delete_duplicate()

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

start_date = date(2019, 1, 1)
end_date = date(2021, 12, 15)
for single_date in daterange(start_date, end_date):
    variFecha = single_date.strftime("%Y-%m-%d")
    print(variFecha)
    run(variFecha)
