from google.cloud import bigquery

client = bigquery.Client()


# Set the encryption key to use for the destination.
# TODO: Replace this key with a key you have created in KMS.
#kms_key_name = "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}".format(
#     "cloud-samples-tests", "us", "test", "test"
# )
job_config = bigquery.LoadJobConfig(
    autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
)
uri = "gs://vtex/CATALOG_API/list_product_id/HistoryGetProductID/0_productID_categoryID_441.json"
load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.
load_job.result()  # Waits for the job to complete.
destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))





table_id = "shopstar-datalake.landing_zone.prueba2"