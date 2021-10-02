import requests
import json
count = 0
mensajeError = {'Message': 'The request is invalid.'}

def get_sku(id):
  url = "https://mercury.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/"""+str(id)+""
  headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
  response = requests.request("GET", url, headers=headers)
  jsonF = json.loads(response.text)
  return jsonF

def operacion_fenix(count):
	f_01 = [1,2,3]
	for i in f_01:
		count += 1
		sku = get_sku(i)
	print(str(count)+" registro almacenado.")
	print(sku)

operacion_fenix(count)
