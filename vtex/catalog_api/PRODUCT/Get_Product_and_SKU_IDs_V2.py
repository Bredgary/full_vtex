import io
from google.cloud import bigquery
client = bigquery.Client()

table_id = "shopstar-datalake.landing_zone.prueba2"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("post_abbr", "STRING"),
    ],
)

body = io.BytesIO(b"Washington,WA")

client.load_table_from_file(body, table_id, job_config=job_config).result()
previous_rows = client.get_table(table_id).num_rows
assert previous_rows > 0

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

uri = "gs://vtex/CATALOG_API/list_product_id/HistoryGetProductID/0_productID_categoryID_441.json"
load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
) 

load_job.result() 

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))