import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from os.path import join
import logging
from datetime import date
import datetime

class init:
  df = pd.DataFrame()
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
  dt = datetime.datetime.today()
  year = dt.year
  month = dt.month
  day = dt.day
  
def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

def get_product_cal():
    try:
        print("Cargando consulta")
        client = bigquery.Client()
        QUERY = ('SELECT distinct productId, lastChange FROM `shopstar-datalake.staging_zone.shopstar_order_items` WHERE lastChange BETWEEN "'+str(init.year)+'-'+str(init.month)+'-'+str(init.day)+' 00:00:00" AND "'+str(init.year)+'-'+str(init.month)+'-'+str(init.day)+' 06:00:00"')
        query_job = client.query(QUERY)
        rows = query_job.result()
        registro = 0
        for row in rows:
            print(row.productId)
            url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/productget/"+str(row.productId)+""
            response = requests.request("GET", url, headers=init.headers)
            Fjson = json.loads(response.text)
            
            taxCode = Fjson["TaxCode"]
            isActive = Fjson["IsActive"]
            title = Fjson["Title"]
            showWithoutStock = Fjson["ShowWithoutStock"]
            keyWords = Fjson["KeyWords"]
            supplierId = Fjson["SupplierId"]
            descriptionShort = Fjson["DescriptionShort"]
            description = Fjson["Description"]
            isVisible = Fjson["IsVisible"]
            metaTagDescription = Fjson["MetaTagDescription"]
            releaseDate = Fjson["ReleaseDate"]
            lomadeeCampaignCode = Fjson["LomadeeCampaignCode"]
            id = Fjson["Id"]
            linkId = Fjson["LinkId"]
            brandId = Fjson["BrandId"]
            refId = Fjson["RefId"]
            listStoreId = Fjson["ListStoreId"]
            categoryId = Fjson["CategoryId"]
            adWordsRemarketingCode = Fjson["AdWordsRemarketingCode"]
            departmentId = Fjson["DepartmentId"]
            name = Fjson["Name"]
            
            df1 = pd.DataFrame({
                'taxCode': taxCode,
                'isActive': isActive,
                'title': title,
                'showWithoutStock': showWithoutStock,
                'keyWords': keyWords,
                'supplierId': supplierId,
                'descriptionShort': descriptionShort,
                'description': description,
                'isVisible': isVisible,
                'metaTagDescription': metaTagDescription,
                'releaseDate': releaseDate,
                'lomadeeCampaignCode': lomadeeCampaignCode,
                'id': id,
                'linkId': linkId,
                'brandId': brandId,
                'refId': refId,
                'listStoreId': listStoreId,
                'categoryId': categoryId,
                'adWordsRemarketingCode': adWordsRemarketingCode,
                'departmentId': departmentId,
                'name': name}, index=[0])
            init.df = init.df.append(df1)
            registro += 1
            print("Registro: "+str(registro))
            if registro == 50:
                run()
        run()
    except:
        print("Error.")
        logging.exception("message")


def delete_duplicate():
  try:
    print("Eliminando duplicados")
    client = bigquery.Client()
    QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_product` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_product`')
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
           
        
        table_schema = [
        {
            "name": "taxCode",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "isActive",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "title",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "showWithoutStock",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "keyWords",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "supplierId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "descriptionShort",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "description",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "isVisible",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "metaTagDescription",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "releaseDate",
            "type": "TIMESTAMP",
            "mode": "NULLABLE"
        },{
            "name": "lomadeeCampaignCode",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "id",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "linkId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "brandId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "refId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ListStoreId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "categoryId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "adWordsRemarketingCode",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "departmentId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "name",
            "type": "STRING",
            "mode": "NULLABLE"
        }]
        
        project_id = '999847639598'
        dataset_id = 'staging_zone'
        table_id = 'shopstar_vtex_product_context'
        
        if df.empty:
            print('DataFrame is empty!')
        else:
            try:
                client  = bigquery.Client(project = project_id)
                dataset  = client.dataset(dataset_id)
                table = dataset.table(table_id)
                job_config = bigquery.LoadJobConfig()
                job_config.schema = format_schema(table_schema)
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                job = client.load_table_from_json(json_object, table, job_config = job_config)
                print(job.result())
                delete_duplicate()
            except:
                client  = bigquery.Client(project = project_id)
                dataset  = client.dataset(dataset_id)
                table = dataset.table(table_id)
                job_config = bigquery.LoadJobConfig()
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                job = client.load_table_from_json(json_object, table, job_config = job_config)
                print(job.result())
                delete_duplicate()
    except:
        print("Error.")
        logging.exception("message")

get_product_cal()