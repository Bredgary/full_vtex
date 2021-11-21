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

def get_sk_context(id,reg):
    try:
        url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyid/"+str(id)+""
        querystring = {"sc":"1"}
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        del Fjson["ProductCategories"]
        data_items = Fjson.items()
        data_list = list(data_items)
        init.df = pd.DataFrame(data_list)
        '''
        for x in Fjson:
            print(x)
            
            df1 = pd.DataFrame({
                'Id': Fjson["Id"],
                'ProductId': Fjson["ProductId"],
                'NameComplete': Fjson["NameComplete"],
                'ComplementName': Fjson["ComplementName"],
                'ProductName': Fjson["ProductName"],
                'ProductDescription': Fjson["ProductDescription"],
                'ProductRefId': Fjson["ProductRefId"],
                'TaxCode': Fjson["TaxCode"],
                'SkuName': Fjson["SkuName"],
                'IsActive': Fjson["IsActive"],
                'IsTransported': Fjson["IsTransported"],
                'IsInventoried': Fjson["IsInventoried"],
                'IsGiftCardRecharge': Fjson["IsGiftCardRecharge"],
                'ImageUrl': Fjson["ImageUrl"],
                'DetailUrl': Fjson["DetailUrl"],
                'CSCIdentification': Fjson["CSCIdentification"],
                'BrandId': Fjson["BrandId"],
                'BrandName': Fjson["BrandName"],
                'IsBrandActive': Fjson["IsBrandActive"],
                'Dimension': Fjson["Dimension"],
                'ManufacturerCode': Fjson["ManufacturerCode"],
                'IsKit': Fjson["IsKit"],
                'KitItems': Fjson["KitItems"],
                'Services': Fjson["Services"],
                'Categories': Fjson["Categories"],
                'CategoriesFullPath': Fjson["CategoriesFullPath"],
                'Attachments': Fjson["Attachments"],
                'Collections': Fjson["Collections"],
                'SkuSellers': Fjson["SkuSellers"],
                'SalesChannels': Fjson["SalesChannels"],
                'Images': Fjson["Images"],
                'Videos': Fjson["Videos"],
                'SkuSpecifications': Fjson["SkuSpecifications"],
                'ProductSpecifications': Fjson["ProductSpecifications"],
                'ProductClusterHighlights': Fjson["ProductClusterHighlights"],
                'ProductCategoryIds': Fjson["ProductCategoryIds"],
                'CommercialConditionId': Fjson["CommercialConditionId"],
                'RewardValue': Fjson["RewardValue"],
                'AlternateIds': Fjson["AlternateIds"],
                'AlternateIdValues': Fjson["AlternateIdValues"],
                'EstimatedDateArrival': Fjson["EstimatedDateArrival"],
                'MeasurementUnit': Fjson["MeasurementUnit"],
                'UnitMultiplier': Fjson["UnitMultiplier"],
                'InformationSource': Fjson["InformationSource"],
                'ModalType': Fjson["ModalType"],
                'KeyWords': Fjson["KeyWords"],
                'ReleaseDate': Fjson["ReleaseDate"],
                'ProductIsVisible': Fjson["ProductIsVisible"],
                'ShowIfNotAvailable': Fjson["ShowIfNotAvailable"],
                'IsProductActive': Fjson["IsProductActive"],
                'ProductFinalScore': Fjson["ProductFinalScore"]}, index=[0])
                '''
            #init.df = init.df.append(df1)
        print("Registro: "+str(reg))
    except:
        print("Vacio")


def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_SKU_ID`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 0
    for row in rows:
        get_sk_context(944,registro)
        registro += 1
        if registro == 1:
            break



def delete_duplicate():
    client = bigquery.Client()
    QUERY = (
        'CREATE OR REPLACE TABLE `shopstar-datalake.landing_zone.shopstar_vtex_sku_context` AS SELECT DISTINCT * FROM `shopstar-datalake.landing_zone.shopstar_vtex_sku_context`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    print(rows)

def run():
    get_params()
    
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)

    project_id = '999847639598'
    dataset_id = 'landing_zone'
    table_id = 'shopstar_vtex_sku_context_'

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
