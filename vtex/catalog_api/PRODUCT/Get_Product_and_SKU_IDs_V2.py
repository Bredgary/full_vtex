#import io
from google.cloud import bigquery
client = bigquery.Client()

#body = io.BytesIO(b"Washington,WA")
filename = '/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/0_productID_categoryID_441.json'
table_id = 'shopstar_vtex_prueba'
dataset_id = 'landing_zone'
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

#client.load_table_from_file(body, table_id, job_config=job_config).result()
#previous_rows = client.get_table(table_id).num_rows
#assert previous_rows > 0

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

job_config.autodetect = True
with open(filename, "rb") as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_ref,
        location="southamerica-east1",job_config=job_config,)

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))
