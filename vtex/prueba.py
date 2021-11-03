import requests
headers={'x-api-key':'dapi879a141a514273c14060301b39ff2ce2'}
url = "https://<databricks-instance>/api/2.1/jobs/list"
response = requests.request("GET", url, headers=headers)

print(response.text)