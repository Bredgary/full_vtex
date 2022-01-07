import pandas as pd
import numpy as np
from google.cloud import bigquery
import os, json
from datetime import datetime
import requests
from datetime import datetime, timezone
from os.path import join
import logging


def run():
  try:
    print("Creacion dm_customer")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_customer` AS
    SELECT
    id customer_id,
    email,
    userId,
    firstName,
    lastName,
    document,
    localeDefault,
    attach,
    accountId,
    accountName,
    dataEntityId,
    createdBy,
    createdIn,
    updatedBy,
    beneficio2,
    crearGiftcard,
    profilePicture,
    proteccionDatos,
    terminosCondiciones,
    terminosPago,
    tradeName,
    rclastcart,
    rclastsession,
    rclastsessiondate,
    homePhone,
    phone,
    stateRegistration,
    approved,
    birthDate,
    businessPhone,
    corporateDocument,
    corporateName,
    documentType,
    gender,
    customerClass,
    priceTables,
    beneficio,
    updatedIn,
    lastInteractionBy,
    lastInteractionIn
    FROM `shopstar-datalake.staging_zone.shopstar_vtex_client`''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_cosutomer actualizado exitosamente")
    create_customer_history()
  except:
    print("Error create_customer!!")
    logging.exception("message")
    
def create_customer_history():
  try:
    print("Creacion dm_customer_history")
    client = bigquery.Client()
    QUERY = ('''
    CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.dm_customer_history` AS 
    SELECT *
    FROM (
    SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY createdIn) row_number
    FROM `shopstar-datalake.cons_zone.dm_customer`)
    WHERE row_number = 1 ''')
    query_job = client.query(QUERY)
    rows = query_job.result()
    print("dm_cosutomer_history actualizado exitosamente")
  except:
    print("Error create_customer_history!!")
    logging.exception("message")
    
run()