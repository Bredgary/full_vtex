f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/delimitador.txt','r')
data_from_string = f_01.read()
delimitador = int(data_from_string)
count = 0
mensajeError = '"CategoryId Not Found"'


def get_sku(id,count,delimitador):
	jsonF = {}
	if count >= delimitador:
		try:
			url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/specification/groupbycategory/"+str(id)+""
			headers = {"Content-Type": "application/json","Accept": "application/json","X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA","X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
			response = requests.request("GET", url, headers=headers)
			if response.text != mensajeError:
				jsonF = json.loads(response.text)
				string = json.dumps(jsonF)
				text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SPECIFICATION_GROUP/SPECIFICATION_GROUP/"+str(count)+"_sku.json", "w")
				text_file.write(string)
				text_file.close()
				print("Get_SKU Terminando: "+str(count))
		except:
			delimitador = count
			text_file = open("/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/delimitador.txt", "w")
			text_file.write(str(delimitador))
			text_file.close()
			system("python3 List_Specifications_Group_by_Category.py")


def operacion_fenix(count):
	f_01 = open ('/home/bred_valenzuela/full_vtex/vtex/catalog_api/SKU/id_sku.json','r')
	data_from_string = f_01.read()
	data_from_string = data_from_string.replace('"', '')
	listaIDS = json.loads(data_from_string)
	for i in listaIDS:
		count += 1
		get_sku(i,count,delimitador)
	print(str(count)+" registro almacenado.")

operacion_fenix(count)