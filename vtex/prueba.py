import requests

DOMAIN = 'https://4191376270202696.6.gcp.databricks.com/'
TOKEN = 'dapi879a141a514273c14060301b39ff2ce2'

response = requests.post(
  'https://%s/api/2.0/clusters/create' % (DOMAIN),
  headers={'Authorization': 'Bearer %s' % TOKEN},
  json={
    "cluster_name": "shopstar",
    "spark_version": "3.1.1",
    "node_type_id": "n1-standard-4",
    "spark_env_vars": {
      "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "num_workers": "8.3"
  }
)

if response.status_code == 200:
  print(response.json()['cluster_id'])
else:
  print("Error launching cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))