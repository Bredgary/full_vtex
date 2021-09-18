from google.cloud import bigquery

GOOGLE_APPLICATION_CREDENTIALS="[8f5d1e898a7c53b21764609e8af700b844cc278f]"

client = bigquery.Client()

# Perform a query.
QUERY = (
    'SELECT id FROM `shopstar-datalake.landing_zone.shopstar_vtex_category` '
    'LIMIT 5')

query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.id)