import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone

class init:
    productList = []
    df = pd.DataFrame()
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}

def get_product(id,reg):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/products/productget/"+str(id)+""
    response = requests.request("GET", url, headers=init.headers)
    Fjson = json.loads(response.text)
    init.productList.append(Fjson)
    print("Registro: "+str(reg))

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_product_ID`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 1
    for row in rows:
        get_product(row.id,registro)
        registro += 1
        if registro == 15:
            break
    

def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
    return formatted_schema

#def delete_duplicate():
    #client = bigquery.Client()
    #QUERY = (
    #    'CREATE OR REPLACE TABLE `shopstar-datalake.landing_zone.shopstar_vtex_product_context` AS SELECT DISTINCT * FROM `shopstar-datalake.landing_zone.shopstar_vtex_product_context`')
    #query_job = client.query(QUERY)  
    #rows = query_job.result()
    #print(rows)

def run():
    get_params()
    
    for x in init.productList:
        df1 = pd.DataFrame({
            'id': x["Id"],
            'name': x["Name"],
            'departmentId': x["DepartmentId"],
            'categoryId': x["CategoryId"],
            'brandId': x["BrandId"],
            'linkId': x["LinkId"],
            'refId': x["RefId"],
            'isVisible': x["IsVisible"],
            'description': x["Description"],
            'descriptionShort': x["DescriptionShort"],
            'releaseDate': x["ReleaseDate"],
            'keyWords': x["KeyWords"],
            'title': x["Title"],
            'isActive': x["IsActive"],
            'taxCode': x["TaxCode"],
            'metaTagDescription': x["MetaTagDescription"],
            'supplierId': x["SupplierId"],
            'showWithoutStock': x["ShowWithoutStock"],
            'ListStoreId': x["ListStoreId"],
            'adWordsRemarketingCode': x["AdWordsRemarketingCode"],
            'lomadeeCampaignCode': x["LomadeeCampaignCode"]}, index=[0])
        init.df = init.df.append(df1)

    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    table_schema = [
        {
            "name": "Id",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "Name",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "DepartmentId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "CategoryId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "BrandId",
            "type": "INTEGER",
            "mode": "NULLABLE"
        },{
            "name": "LinkId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "RefId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "IsVisible",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "Description",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "DescriptionShort",
            "type": "FLOAT",
            "mode": "NULLABLE"
        },{
            "name": "ReleaseDate",
            "type": "DATE",
            "mode": "NULLABLE"
        },{
            "name": "KeyWords",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "Title",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "IsActive",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "TaxCode",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "MetaTagDescription",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "SupplierId",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "ShowWithoutStock",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },{
            "name": "ListStoreId",
            "type": "INTEGER",
            "mode": "REPEATED"
        },{
            "name": "AdWordsRemarketingCode",
            "type": "STRING",
            "mode": "NULLABLE"
        },{
            "name": "LomadeeCampaignCode",
            "type": "STRING",
            "mode": "NULLABLE"
        }]

    project_id = '999847639598'
    dataset_id = 'landing_zone'
    table_id = 'shopstar_vtex_product_context'

    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = format_schema(table_schema)
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    #delete_duplicate()
    
run()