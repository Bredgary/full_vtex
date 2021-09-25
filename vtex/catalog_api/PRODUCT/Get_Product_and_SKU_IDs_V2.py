from google.cloud import bigquery


client = bigquery.Client()
table_id = "shopstar-datalake.landing_zone.test"


job_config = bigquery.LoadJobConfig(autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
uri = "gs://vtex/CATALOG_API/list_product_id/HistoryGetProductID/prueba.json"
load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
load_job.result() 
destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows)) 