import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join

class init:
    productList = []
    df = pd.DataFrame()
    Id = None
    Name = None
    IsActive = None
    ProductClusterId = None
    CountryCode = None
    CultureInfo = None
    TimeZone = None
    CurrencyCode = None
    CurrencySymbol = None
    CurrencyLocale = None
    CurrencyFormatInfo = None
    CurrencyDecimalDigits = None
    CurrencyDecimalSeparator = None
    CurrencyGroupSeparator = None
    CurrencyGroupSize = None
    StartsWithCurrencySymbol = None
    Origin = None
    Position = None
    ConditionRule = None
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
    
        
def get_list_channel():
    url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/saleschannel/list"
    response = requests.request("GET", url, headers=init.headers)
    Fjson = json.loads(response.text)
    for x in Fjson:
	    init.Id = x["Id"]
	    init.Name = x["Name"]
	    init.IsActive = x["IsActive"]
	    init.ProductClusterId = x["ProductClusterId"]
	    init.CountryCode = x["CountryCode"]
	    init.CultureInfo = x["CultureInfo"]
	    init.TimeZone = x["TimeZone"]
	    init.CurrencyCode = x["CurrencyCode"]
	    init.CurrencySymbol = x["CurrencySymbol"]
	    init.CurrencyLocale = x["CurrencyLocale"]
	    CurrencyFormatInfo = x["CurrencyFormatInfo"]
	    init.CurrencyDecimalDigits = CurrencyFormatInfo["CurrencyDecimalDigits"]
	    init.CurrencyDecimalSeparator = CurrencyFormatInfo["CurrencyDecimalSeparator"]
	    init.CurrencyGroupSeparator = CurrencyFormatInfo["CurrencyGroupSeparator"]
	    init.CurrencyGroupSize = CurrencyFormatInfo["CurrencyGroupSize"]
	    init.StartsWithCurrencySymbol = CurrencyFormatInfo["StartsWithCurrencySymbol"]
	    init.Origin = x["Origin"]
	    init.Position = x["Position"]
	    init.ConditionRule = x["ConditionRule"]
    
    df1 = pd.DataFrame({
        'Id': init.Id,
        'Name': init.Name,
        'IsActive': init.IsActive,
        'ProductClusterId': init.ProductClusterId,
        'CountryCode': init.CountryCode,
        'CultureInfo': init.CultureInfo,
        'TimeZone': init.TimeZone,
        'CurrencyCode': init.CurrencyCode,
        'CurrencySymbol': init.CurrencySymbol,
        'CurrencyLocale': init.CurrencyLocale,
        'CurrencyDecimalDigits': init.CurrencyDecimalDigits,
        'CurrencyDecimalSeparator': init.CurrencyDecimalSeparator,
        'CurrencyGroupSeparator': init.CurrencyGroupSeparator,
        'CurrencyGroupSize': init.CurrencyGroupSize,
        'StartsWithCurrencySymbol': init.StartsWithCurrencySymbol,
        'Origin': init.Origin,
        'Position': init.Position,
        'ConditionRule': init.ConditionRule}, index=[0])
    init.df = init.df.append(df1)
        

def run():
    get_list_channel()
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    project_id = '999847639598'
    dataset_id = 'staging_zone'
    table_id = 'shopstar_vtex_sales_channel_list'
    
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