import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from datetime import date, timedelta
import logging

class init:
	df = pd.DataFrame()

def cl_client(fecha):
	url = "https://mercury.vtexcommercestable.com.br/api/dataentities/CL/search"
	querystring = {"_fields":"profilePicture,proteccionDatos,terminosCondiciones,terminosPago,tradeName,homePhone,phone,stateRegistration,email,userId,firstName,lastName,document,attach,approved,birthDate,businessPhone,corporateDocument,corporateName,documentType,gender,priceTables,id,accountId,accountName,dataEntityId,createdBy,createdIn,updatedBy,updatedIn,lastInteractionBy,lastInteractionIn","_where":"createdIn="+fecha+""}
	headers = {"Content-Type": "application/json","Accept": "application/vnd.vtex.ds.v10+json","REST-Range": "resources=0-1000","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	Fjson = json.loads(response.text)
	for x in Fjson:
		df1 = pd.DataFrame({
			'email': x["email"],
			'userId': x["userId"],
			'firstName': x["firstName"],
			'lastName': x["lastName"],
			'document': x["document"],
			'attach': x["attach"],
			'approved': x["approved"],
			'birthDate': x["birthDate"],
			'businessPhone': x["businessPhone"],
			'corporateDocument': x["corporateDocument"],
			'corporateName': x["corporateName"],
			'documentType': x["documentType"],
			'gender': x["gender"],
			'priceTables': x["priceTables"],
			'id': x["id"],
			'accountId': x["accountId"],
			'accountName': x["accountName"],
			'dataEntityId': x["dataEntityId"],
			'createdBy': x["createdBy"],
			'createdIn': x["createdIn"],
			'updatedBy': x["updatedBy"],
			'updatedIn': x["updatedIn"],
			'lastInteractionBy': x["lastInteractionBy"],
			'lastInteractionIn': x["lastInteractionIn"],
			'profilePicture': x["profilePicture"],
			'proteccionDatos': x["proteccionDatos"],
			'terminosCondiciones': x["terminosCondiciones"],
			'terminosPago': x["terminosPago"],
			'tradeName': x["tradeName"],
			'homePhone': x["homePhone"],
			'phone': x["phone"],
			'stateRegistration': x["stateRegistration"]}, index=[0])
		init.df = init.df.append(df1)

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
			'CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_vtex_client_test` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_vtex_client_test`')
		query_job = client.query(QUERY)
		rows = query_job.result()
		print(rows)
	except:
		print("Consulta SQL no ejecutada")

def run():
	try:
		df = init.df
		
		df.reset_index(drop=True, inplace=True)
		json_data = df.to_json(orient = 'records')
		json_object = json.loads(json_data)
		
		'''
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
		  }
		  '''
		
		project_id = '999847639598'
		dataset_id = 'test'
		table_id = 'shopstar_vtex_client_test'
		
		if df.empty:
			print('DataFrame is empty!')
		else:
			client  = bigquery.Client(project = project_id)
			dataset  = client.dataset(dataset_id)
			table = dataset.table(table_id)
			job_config = bigquery.LoadJobConfig()
			job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
			#job_config.schema = format_schema(table_schema)
			job_config.write_disposition = "WRITE_TRUNCATE"
			job_config.autodetect = True
			job = client.load_table_from_json(json_object, table, job_config = job_config)
			print(job.result())
			delete_duplicate()
	except:
		print("Error.")
		logging.exception("message")


def daterange(start_date, end_date):
	for n in range(int((end_date - start_date).days)):
		yield start_date + timedelta(n)

def time():	
	start_date = date(2019, 1, 1)
	end_date = date(2022, 1, 5)
	for single_date in daterange(start_date, end_date):
		variFecha = single_date.strftime("%Y-%m-%d")
		print("Dia: "+str(variFecha))
		cl_client(variFecha)
	run()

time()  





	