import requests
headers={'x-api-key':'dapi879a141a514273c14060301b39ff2ce2'}
url = "https://4191376270202696.6.gcp.databricks.com/api/2.1/jobs/list"
response = requests.request("POST", url, headers=headers)

print(response)