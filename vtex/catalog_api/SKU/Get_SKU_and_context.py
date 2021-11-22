import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone

class init:
    df = pd.DataFrame()
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
        
def sku_context(id,reg):
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyid/"+str(id)+""
    querystring = {"sc":"1"}
    response = requests.request("GET", url, headers=init.headers, params=querystring)
    Fjson = json.loads(response.text)
    ids = Fjson["Id"]
    ProductId = Fjson["ProductId"]
    NameComplete = Fjson["NameComplete"]
    ProductName = Fjson["ProductName"]
    ProductDescription = Fjson["ProductDescription"]
    SkuName = Fjson["SkuName"]
    IsActive = Fjson["IsActive"]
    IsTransported = Fjson["IsTransported"]
    IsInventoried = Fjson["IsInventoried"]
    IsGiftCardRecharge = Fjson["IsGiftCardRecharge"]
    ImageUrl = Fjson["ImageUrl"]
    DetailUrl = Fjson["DetailUrl"]
    CSCIdentification = Fjson["CSCIdentification"]
    BrandId = Fjson["BrandId"]
    BrandName = Fjson["BrandName"]
    Dimension = Fjson["Dimension"]
    if Dimension:
        dimension_cubicweight = Dimension["cubicweight"]
        dimension_height = Dimension["height"]
        dimension_length = Dimension["length"]
        dimension_weight = Dimension["weight"]
    else:
        dimension_cubicweight = None
        dimension_height = None
        dimension_length = None
        dimension_weight = None
    RealDimension = Fjson["RealDimension"]
    if RealDimension:
        RealDimension_realCubicWeight = RealDimension["realCubicWeight"]
        RealDimension_realHeight = RealDimension["realHeight"]
        RealDimension_realLength = RealDimension["realLength"]
        RealDimension_realWeight = RealDimension["realWeight"]
        RealDimension_realWidth = RealDimension["realWidth"]
    else:
        RealDimension_realCubicWeight = None
        RealDimension_realHeight = None
        RealDimension_realLength = None
        RealDimension_realWeight = None
        RealDimension_realWidth = None
    Attachments = Fjson["Attachments"]
    if Attachments:
        Attachments_Id = Attachments["Id"]
        Attachments_Name = Attachments["Name"]
        Attachments_IsActive = Attachments["IsActive"]
        Attachments_IsRequired = Attachments["IsRequired"]
    else:
        Attachments_Id = None
        Attachments_Name = None
        Attachments_IsActive = None
        Attachments_IsRequired = None
    SkuSellers = Fjson["SkuSellers"]
    if SkuSellers:
        SkuSellers_SellerId = SkuSellers[0]
        SkuSellers_StockKeepingUnitId = SkuSellers[1]
        SkuSellers_SellerStockKeepingUnitId = SkuSellers[2]
        SkuSellers_IsActive = SkuSellers[3]
        SkuSellers_FreightCommissionPercentage = SkuSellers[4]
        SkuSellers_ProductCommissionPercentage = SkuSellers[5]
    else:
        SkuSellers_SellerId = None
        SkuSellers_StockKeepingUnitId = None
        SkuSellers_SellerStockKeepingUnitId = None
        SkuSellers_IsActive = None
        SkuSellers_FreightCommissionPercentage = None
        SkuSellers_ProductCommissionPercentage = None
    Images = Fjson["Images"]
    if Images:
        Images_ImageUrl = Images["ImageUrl"]
        Images_ImageName = Images["ImageName"]
        Images_FileId = Images["FileId"]
    else:
        Images_ImageUrl = None
        Images_ImageName = None
        Images_FileId = None
    AlternateIds = Fjson["AlternateIds"]
    if AlternateIds:
        AlternateIds_Ean = AlternateIds["Ean"]
        AlternateIds_RefId = AlternateIds["RefId"]
    else:
        AlternateIds_Ean = None
        AlternateIds_RefId = None
    #try:
    df1 = pd.DataFrame({
        'Id': ids,
        'ProductId': ProductId,
        'NameComplete': NameComplete,
        'ProductName': ProductName,
        'ProductDescription': ProductDescription,
        'SkuName': SkuName,
        'IsActive': IsActive,
        'IsTransported': IsTransported,
        'IsInventoried': IsInventoried,
        'IsGiftCardRecharge': IsGiftCardRecharge,
        'ImageUrl': ImageUrl,
        'DetailUrl': DetailUrl,
        'CSCIdentification': CSCIdentification,
        'BrandId': BrandId,
        'dimension_cubicweight': dimension_cubicweight,
        'dimension_height': dimension_height,
        'dimension_length': dimension_length,
        'dimension_weight': dimension_weight,
        'RealDimension_realCubicWeight': RealDimension_realCubicWeight,
        'RealDimension_realHeight': RealDimension_realHeight,
        'RealDimension_realLength': RealDimension_realLength,
        'RealDimension_realWeight': RealDimension_realWeight,
        'RealDimension_realWidth': RealDimension_realWidth,
        'Attachments_Id': Attachments_Id,
        'Attachments_Name': Attachments_Name,
        'Attachments_IsActive': Attachments_IsActive,
        'Attachments_IsRequired': Attachments_IsRequired,
        'SkuSellers_SellerId' : SkuSellers_SellerId,
        'SkuSellers_StockKeepingUnitId' : SkuSellers_StockKeepingUnitId,
        'SkuSellers_SellerStockKeepingUnitId' : SkuSellers_SellerStockKeepingUnitId,
        'SkuSellers_IsActive' : SkuSellers_IsActive,
        'SkuSellers_FreightCommissionPercentage' : SkuSellers_FreightCommissionPercentage,
        'SkuSellers_ProductCommissionPercentage' : SkuSellers_ProductCommissionPercentage,
        'Images_ImageUrl' : Images_ImageUrl,
        'Images_ImageName' : Images_ImageName,
        'Images_FileId' : Images_FileId,
        'AlternateIds_Ean' : AlternateIds_Ean,
        'AlternateIds_RefId' : AlternateIds_RefId,
        'BrandName': BrandName}, index=[0])
    init.df = init.df.append(df1)
    print("Registro: "+str(reg))
    #except:
    #    print("Vacio")


def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = (
        'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_category`')
    query_job = client.query(QUERY)  
    rows = query_job.result()
    registro = 1
    for row in rows:
        sku_context(268978,registro)
        registro += 1
        break
    
def delete_duplicate():
    try:
        print("Borrando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.landing_zone.shopstar_vtex_sku_context` AS SELECT DISTINCT * FROM `shopstar-datalake.landing_zone.shopstar_vtex_sku_context`')
        query_job = client.query(QUERY)  
        rows = query_job.result()
        print(rows)
    except:
        print("Query no ejecutada")

def run():
    #try:
        get_params()
        df = init.df
        df.reset_index(drop=True, inplace=True)
        json_data = df.to_json(orient = 'records')
        json_object = json.loads(json_data)
        
        project_id = '999847639598'
        dataset_id = 'landing_zone'
        table_id = 'shopstar_vtex_sku_context'
    
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
    #except:
    #    print("vacio")
    
run()