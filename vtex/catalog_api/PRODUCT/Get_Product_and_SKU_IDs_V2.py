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
import self

table = "prueba"
data = "test"
project_id = "shopstar-datalake"
dataset_id = "landing_zone"

my_schema = [
  {
    "name": "Id",
    "type": "STRING"
  }
]
stream_data(self,table,table,my_schema)

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
    self.service.tabledata().insertAll(projectId=project_id),datasetId=dataset_id,tableId=table,body=body).execute(num_retries=5)



