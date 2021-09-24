import uuid
def stream_data(self, table, data, schema):
    # first checks if table already exists. If it doesn't, then create it
    r = self.service.tables().list(projectId=your_project_id,
                                     datasetId=your_dataset_id).execute()
    table_exists = [row['tableReference']['tableId'] for row in
                    r['tables'] if
                    row['tableReference']['tableId'] == table]
    if not table_exists:
        body = {
            'tableReference': {
                'tableId': table,
                'projectId': your_project_id,
                'datasetId': your_dataset_id
            },
            'schema': schema
        }
        self.service.tables().insert(projectId=your_project_id,
                                     datasetId=your_dataset_id,
                                     body=body).execute()

    # with table created, now we can stream the data
    # to do so we'll use the tabledata().insertall() function.
    body = {
        'rows': [
            {
                'json': data,
                'insertId': str(uuid.uuid4())
            }
        ]
    }
    self.service.tabledata().insertAll(projectId=your_project_id),
                                       datasetId=your_dataset_id,
                                       tableId=table,
                                         body=body).execute(num_retries=5)