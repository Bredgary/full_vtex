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

def get_params():
    try:
        client = bigquery.Client()
        QUERY = ('CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.device` AS SELECT GENERATE_UUID() id_device,visitorId id_visitor,device.browser browser,    device.browserVersion browserVersion,device.browserSize browserSize,    device.operatingSystem operatingSystem,device.operatingSystemVersion operatingSystemVersion,device.isMobile isMobile,device.mobileDeviceBranding mobileDeviceBranding,device.mobileDeviceModel mobileDeviceModel,device.mobileInputSelector mobileInputSelector,device.mobileDeviceInfo mobileDeviceInfo,device.mobileDeviceMarketingName mobileDeviceMarketingName,device.flashVersion flashVersion,device.javaEnabled javaEnabled,device.language language,device.screenColors screenColors,device.screenResolution screenResolution,device.deviceCategory deviceCategory FROM `shopstar-datalake.191656782.ga_sessions_20211130` ')
        query_job = client.query(QUERY)
        delete_duplicate()
    except:
        print("Consulta SQL no ejecutada")

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.device` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.device`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

get_params()