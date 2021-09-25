from google.cloud import bigquery
import json

client = bigquery.Client()


# Set the encryption key to use for the destination.
# TODO: Replace this key with a key you have created in KMS.
#kms_key_name = "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}".format(
#     "cloud-samples-tests", "us", "test", "test"
# )


f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/PRODUCT/HistoryGetProductID/0_productID_categoryID_441.json','r')
data_from_string = f_01.read()

temp = json.loads(data_from_string)
print(type(temp))



table_id = "shopstar-datalake.landing_zone.prueba2"

client = bigquery.Client()
filename = temp
dataset_id = 'landing_zone'
table_id = 'prueba2'
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.autodetect = True
with open(filename, "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location="southamerica-east1",  # Must match the destination dataset location.
    job_config=job_config,)  # API request
job.result()  # Waits for table load to complete.
print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
print("finalizado")
