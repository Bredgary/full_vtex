#!/usr/bin/python
# -*- coding: UTF-8 -*-
import requests
import json
import os
import re
from datetime import datetime
from os import system
from google.cloud import bigquery
import numpy as np
import cv2 as cv

img = np.zeros ((320, 320, 3), np.uint8)
imprimir img.shape

point_size = 1
point_color = (0, 0, 255) 
espesor = 4 
points_list = [(160, 160), (136, 160), (150, 200), (200, 180), (120, 150), (145, 180)]


print("comenzando_trabajo") 

def get_list_order(creationDateFrom,creationDateTo):
	url = "https://mercury.vtexcommercestable.com.br/api/oms/pvt/orders"
	querystring = {"f_creationDate":"creationDate:[creationDate:["+creationDateFrom+"T02:00:00.000Z TO "+creationDateTo+"T01:59:59.999Z]]","f_hasInputInvoice":"false"}
	headers = {
		"Accept": "application/json",
		"Content-Type": "application/json",
		"X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
		"X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	
	text_file = open("/home/bred_valenzuela/full_vtex/vtex/orders/ORDERS/JsonHistory/ordenes-"+creationDateFrom+"-"+creationDateTo+".json", "w")
	text_file.write(response.text)
	text_file.close() 
	for point in points_list:
		cv.circle(img, point, point_size, point_color, thickness)
		cv.circle(img, (160, 160), 60, point_color, 0)
		cv.namedWindow("image")
		cv.imshow('image', img)
		cv.waitKey (10000) 
	cv.destroyAllWindows()

get_list_order("2021-01-01","2021-01-10")
print("Primera semana finalizada")
get_list_order("2021-01-10","2021-01-17")
print("Segunda semana finalizada")
get_list_order("2021-01-17","2021-01-24")
print("Tercera semana finalizada")
get_list_order("2021-01-24","2021-01-31")

print("Estado Finalizado")

