from google.cloud import bigquery


client = bigquery.Client()

# Perform a query.
QUERY = (
    'SELECT id, count(id) as total FROM `shopstar-datalake.landing_zone.shopstar_vtex_category` ')

query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

print("total={}".format(row["total"]))

for row in rows:
    string =  json.dumps(row.id)
    system("touch /home/bred_valenzuela/full_vtex/vtex/catalog_api/idsProducts.json")
    text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/idsProducts.json", "w")
    text_file.write(string)
    text_file.close()