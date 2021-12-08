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
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/collection/search"
    response = requests.request("GET", url, headers=headers)
    Fjson = json.loads(response.text)
    paging = Fjson["paging"]
    page = paging["page"]
    pages = paging["pages"]
    total = paging["total"]
    lista = Fjson["items"]
    
def get_all_collections():
    try:
        for x in range(init.pages):
            x += 1
            querystring = {"page":""+str(x)+"","pageSize":""+str(init.total)+"","orderByAsc":"true"}
            response = requests.request("GET", init.url, headers=init.headers, params=querystring)
            Fjson = init.Fjson
            lista = Fjson["items"]
            for x in lista:
                df1 = pd.DataFrame({
                    'id': x["id"],
    				'name': x["name"],
    				'searchable':x["searchable"],
    				'highlight': x["highlight"],
    				'dateFrom': x["dateFrom"],
    				'dateTo': x["dateTo"],
    				'totalSku': x["totalSku"],
    				'totalProducts': x["totalProducts"],
    				'type': x["type"],
    				'lastModifiedBy': x["lastModifiedBy"]}, index=[0])
                init.df = init.df.append(df1)
                print("registro: "+str(x))
    except:
        print("Vacio")


def delete_duplicate():
    client = bigquery.Client()
    QUERY = (
        'CREATE OR REPLACE TABLE `shopstar-datalake.staging_zone.shopstar_vtex_collection_beta` AS SELECT DISTINCT * FROM `shopstar-datalake.staging_zone.shopstar_vtex_collection_beta`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    print(rows)

def run():
    get_all_collections()
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    

    project_id = '999847639598'
    dataset_id = 'staging_zone'
    table_id = 'shopstar_vtex_collection_beta'

    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = "WRITE_TRUNCATE"
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()
    
run()


