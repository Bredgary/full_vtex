from google.cloud import bigquery

GOOGLE_APPLICATION_CREDENTIALS="/home/bred_valenzuela/full_vtex/vtex/orders/miclave.json"

client = bigquery.Client()

# Perform a query.
QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_category` '
    'LIMIT 5')

query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.id)