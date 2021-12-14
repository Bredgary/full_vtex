import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join

class init:
    
    df = pd.DataFrame()
    '''
    packageAttachment
    '''
    courier = None
    invoiceNumber = None
    invoiceValue = None
    invoiceUrl = None
    issuanceDate = None
    trackingNumber = None
    invoiceKey = None
    trackingUrl = None
    embeddedInvoice = None
    type = None
    cfop = None
    volumes = None
    EnableInferItems = None
    
    headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}


def get_order(id,reg):
    #try:
        url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders/"+str(id)+""
        response = requests.request("GET", url, headers=init.headers)
        Fjson = json.loads(response.text)
        
        '''
        INIT DIMENSION  packageAttachment
        '''    
        try:
            packageAttachment = Fjson["packageAttachment"]
            packages = packageAttachment["packages"]
        except:
            print("packageAttachment. No tiene datos")    
        
        
        try:
            for x in packages:
                init.courier = x["courier"]
                init.invoiceNumber = x["invoiceNumber"]
                init.invoiceValue = x["invoiceValue"]
                init.invoiceUrl = x["invoiceUrl"]
                init.issuanceDate = x["issuanceDate"]
                init.trackingNumber = x["trackingNumber"]
                init.invoiceKey = x["invoiceKey"]
                init.trackingUrl = x["trackingUrl"]
                init.embeddedInvoice = x["embeddedInvoice"]
                init.type = x["type"]
                init.cfop = x["cfop"]
                init.volumes = x["volumes"]
                init.EnableInferItems = x["EnableInferItems"]
        except:
            print("vacio")
    
        
        df1 = pd.DataFrame({
            'orderId': str(id),
            'courier': str(init.courier),
            'invoiceNumber': str(init.invoiceNumber),
            'invoiceValue': str(init.invoiceValue),
            'invoiceUrl': str(init.invoiceUrl),
            'issuanceDate': str(init.issuanceDate),
            'trackingNumber': str(init.trackingNumber),
            'invoiceKey': str(init.invoiceKey),
            'trackingUrl': str(init.trackingUrl),
            'embeddedInvoice': str(init.embeddedInvoice),
            'type': str(init.type),
            'cfop': str(init.cfop),
            'volumes': str(init.volumes),
            'EnableInferItems': str(init.EnableInferItems)}, index=[0])
        init.df = init.df.append(df1)
        print("Registro: "+str(reg))
    #except:
    #    print("vacio")
              

def get_params():
    print("Cargando consulta")
    client = bigquery.Client()
    QUERY = ('SELECT orderId FROM `shopstar-datalake.staging_zone.shopstar_vtex_list_order`')
    query_job = client.query(QUERY)
    rows = query_job.result()
    registro = 0
    for row in rows:
        registro += 1
        get_order("1033163314156-02",registro)
        if registro ==2:
            break
        
def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.test.shopstar_order_package` AS SELECT DISTINCT * FROM `shopstar-datalake.test.shopstar_order_package`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")


def run():
    get_params()
    df = init.df
    df.reset_index(drop=True, inplace=True)
    json_data = df.to_json(orient = 'records')
    json_object = json.loads(json_data)
    
    project_id = '999847639598'
    dataset_id = 'test'
    table_id = 'shopstar_order_package'
    
    client  = bigquery.Client(project = project_id)
    dataset  = client.dataset(dataset_id)
    table = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_object, table, job_config = job_config)
    print(job.result())
    delete_duplicate()
    
run()
