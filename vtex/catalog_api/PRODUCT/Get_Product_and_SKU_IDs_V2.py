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
from google.cloud import bigquery

client = bigquery.Client()


table_id = "shopstar-datalake:landing_zone.prueba"

rows_to_insert = [
    {u"full_name": u"Phred Phlyntstone", u"age": 32},
    {u"full_name": u"Wylma Phlyntstone", u"age": 29},
]

errors = client.insert_rows_json(
    table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert)
) 
if errors == []:
    print("New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}")