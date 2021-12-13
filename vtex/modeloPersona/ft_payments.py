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
        QUERY = ('''CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.ft_payments` AS 
        SELECT 
        id_payments id_payments,
        payments.orderId id_order,
        postalCode,
        billingAddress.city,
        billingAddress.state,
        billingAddress.country,
        billingAddress.street,
        billingAddress.number,
        payments.bankIssuedInvoiceBarCodeNumber bankIssuedInvoiceBarCodeNumber,
        payments.bankIssuedInvoiceIdentificationNumber bankIssuedInvoiceIdentificationNumber,
        payments.accountId accountId, 
        payments.parentAccountId parentAccountId, 
        payments.koinUrl koinUrl,
        payments.giftCardAsDiscount giftCardAsDiscount,
        payments.group,
        payments.dueDate dueDate,
        payments.redemptionCode redemptionCode,
        payments.giftCardProvider giftCardProvider, 
        payments.tid tid,
        payments.giftCardCaption giftCardCaption,
        payments.giftCardName giftCardName,
        payments.bankIssuedInvoiceIdentificationNumberFormatted bankIssuedInvoiceIdentificationNumberFormatted,
        connectorResponses.ReturnCode connectorResponses_ReturnCode,
        connectorResponses.Message connectorResponses_payments_Message,
        connectorResponses.authId connectorResponses_authId,
        payments.expireYear expireYear,
        payments.expireMonth expireMonth,
        payments.firstDigits firstDigits, 
        payments.paymentSystemName paymentSystemName,
        payments.referenceValue referenceValue, 
        payments.cardNumber cardNumber, 
        payments.lastDigits lastDigits, 
        payments.cvv2 cvv2, 
        payments.cardHolder cardHolder,
        payments.giftCardId giftCardId,
        GENERATE_UUID() id,
        payments.bankIssuedInvoiceBarCodeType bankIssuedInvoiceBarCodeType,
        payments.paymentSystem paymentSystem,
        payments.url url,
        payments.value value,
        payments.installments installments,
        transactions.merchantName merchantName, 
        transactions.transactionId transactionId, 
        transactions.isActive isActive, 
        payments.giftCardName giftCard 
        FROM `shopstar-datalake.staging_zone.shopstar_order_payments` as payments
        JOIN (SELECT * FROM `shopstar-datalake.staging_zone.shopstar_order_billingAddress`) as billingAddress
        ON payments.orderId = billingAddress.orderId
        JOIN (SELECT * FROM `shopstar-datalake.staging_zone.shopstar_order_connectorResponses`) as connectorResponses
        ON connectorResponses.orderId = billingAddress.orderId
        JOIN (SELECT * FROM `shopstar-datalake.staging_zone.shopstar_order_transactions`) as transactions
        ON transactions.orderId = billingAddress.orderId
''')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
        delete_duplicate()
    except:
        print("Consulta SQL no ejecutada")

def delete_duplicate():
    try:
        print("Eliminando duplicados")
        client = bigquery.Client()
        QUERY = (
            'CREATE OR REPLACE TABLE `shopstar-datalake.cons_zone.ft_invoices` AS SELECT DISTINCT * FROM `shopstar-datalake.cons_zone.ft_invoices`')
        query_job = client.query(QUERY)
        rows = query_job.result()
        print(rows)
    except:
        print("Consulta SQL no ejecutada")

get_params()