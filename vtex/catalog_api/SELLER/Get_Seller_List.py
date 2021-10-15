url = "https://mercury.vtexcommercestable.com.br/api/catalog_system/pvt/seller/list"

querystring = {"sc":"2","sellerType":"1","isBetterScope":"false"}

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-VTEX-API-AppKey": "vtexappkey-mercury-PKEDGA",
    "X-VTEX-API-AppToken": "OJMQPKYBXPQSXCNQHWECEPDPMNVWAEGFBKKCNRLANUBZGNUWAVLSCIPZGWDCOCBTIKQMSLDPKDOJOEJZTYVFSODSVKWQNJLLTHQVWHEPRVHYTFLBNEJPGWAUHYQIPMBA"
}

response = requests.request("GET", url, headers=headers, params=querystring)