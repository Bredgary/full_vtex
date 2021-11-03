import requests

DOMAIN = 'https://4191376270202696.6.gcp.databricks.com/'
TOKEN = 'dapi879a141a514273c14060301b39ff2ce2'

response = requests.post(
  'https://%s/api/2.1/jobs/list' % (DOMAIN),
  headers={'Authorization': 'Bearer %s' % TOKEN},
)

if response.status_code == 200:
  print(response.json())
else:
  print("Error launching cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))


