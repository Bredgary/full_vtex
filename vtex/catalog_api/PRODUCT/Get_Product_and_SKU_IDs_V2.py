#!/usr/bin/python
# -*- coding: UTF-8 -*-
import requests
import json
import os
import re
from datetime import datetime
from os import system
import time
import sys
import uuid

#credentials = GoogleCredentials.get_application_default()
#service = build('bigquery', 'v2', credentials = credentials)

table = "prueba"
data = "test"
project_id = "shopstar-datalake"
dataset_id = "landing_zone"

data = {u'days_validated': '20', u'days_trained': '80', u'navigated_score': '1', u'description': 'First trial of top seller alg. No filter nor any condition is applied. Skus not present in train count as rank=0.5', u'init_cv_date': '2016-03-06', u'metric': 'rank', u'unix_date': '1461610020241117', u'purchased_score': '10', u'result': '0.32677139316724546', u'date': '2016-04-25', u'carted_score': '3', u'end_cv_date': '2016-03-25'}
schema = {u'fields': [{u'type': u'STRING', u'name': u'date', u'mode': u'NULLABLE'}, {u'type': u'INTEGER', u'name': u'unix_date', u'mode': u'NULLABLE'}, {u'type': u'STRING', u'name': u'init_cv_date', u'mode': u'NULLABLE'}, {u'type': u'STRING', u'name': u'end_cv_date', u'mode': u'NULLABLE'}, {u'type': u'INTEGER', u'name': u'days_trained', u'mode': u'NULLABLE'}, {u'type': u'INTEGER', u'name': u'days_validated', u'mode': u'NULLABLE'}, {u'type': u'INTEGER', u'name': u'navigated_score', u'mode': u'NULLABLE'}, {u'type': u'INTEGER', u'name': u'carted_score', u'mode': u'NULLABLE'}, {u'type': u'INTEGER', u'name': u'purchased_score', u'mode': u'NULLABLE'}, {u'type': u'STRING', u'name': u'description', u'mode': u'NULLABLE'}, {u'type': u'STRING', u'name': u'metric', u'mode': u'NULLABLE'}, {u'type': u'FLOAT', u'name': u'result', u'mode': u'NULLABLE'}]}


def stream_data(self, table, data, schema):
    r = self.service.tables().list(projectId=your_project_id,datasetId=your_dataset_id).execute()
    table_exists = [row['tableReference']['tableId'] for row in r['tables'] if row['tableReference']['tableId'] == table]
    if not table_exists:
        body = {
            'tableReference':
            {
                'tableId': table,
                'projectId': project_id,
                'datasetId': dataset_id
            },'schema': schema
        }
        self.service.tables().insert(projectId=project_id,datasetId=dataset_id,body=body).execute()
        
        body = {'rows': [{'json': data,'insertId': str(uuid.uuid4())}]
    }
   # self.service.tabledata().insertAll(projectId=project_id),datasetId=dataset_id,tableId=table,body=body).execute()