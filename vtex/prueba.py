import requests

url = "https://4191376270202696.6.gcp.databricks.com/api/2.1/jobs/list"

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "DATABRICKS_TOKEN": "dapi879a141a514273c14060301b39ff2ce2"
}

response = requests.request("GET", url, headers=headers)

print(response)