import requests
import json
import pandas as pd
import dataset
import boto3
import os
import boto3
import glob
import datetime

class ShopifyScraper():
    pass

    def __init__(self, baseurl):
        self.baseurl = baseurl
        #return baseurl

    def getwebsitename(self):
        URL_split = self.baseurl.replace('/', '').split(':')
        URL_name = URL_split[1].split('.')[0]
        current_time = datetime.datetime.now()
        current_time_string = current_time.strftime('%m/%d/%Y %H:%M:%S.%f')
        formatted_date = current_time_string.replace("/", "_").replace(":", "_").replace(" ", "_").replace(".", "_")
        return URL_name,formatted_date

    def downloadjson(self, page):
        r = requests.get(self.baseurl + f'products.json?limit=250&page={page}', timeout=5)
        if r.status_code != 200:
            print('Bad Status Code :', r.status_code)
        if len(r.json()['products']) > 0:
            data = r.json()['products']
            return data
        else:
            return

    def parsejson(self, jsondata):
        products = []
        for prod in jsondata:
            mainid = prod['id']
            title = prod['title']
            published_at = prod['published_at']
            product_type = prod['product_type']
            for variant in prod['variants']:
                item = {
                    'id': mainid,
                    'title': title,
                    'published_at': published_at,
                    'product_type': product_type,
                    'varid': variant['id'],
                    'vartitle': variant['title'],
                    'sku': variant['sku'],
                    'price': variant['price'],
                    'available': variant['available'],
                    'product_id': variant['product_id'],
                    'created_at': variant['created_at'],
                    'updated_at': variant['updated_at'],
                    'compare_at_price': variant['compare_at_price']
                }
                products.append(item)
        return products

    def mains(self):
        website_url = ShopifyScraper(self.baseurl)
        Website_Name,Data_Pull_time = website_url.getwebsitename()
        results = []
        for page in range(1, 10):
            data = website_url.downloadjson(page)
            print('Getting Page:', page)
            try:
                results.append(website_url.parsejson(data))
            except:
                print(f'Completed, total pages = {page - 1}')
                break
        json_merged =[]
        for i in results:
            for j in i:
                json_merged.append(j)
        Data = pd.DataFrame(json_merged)
        Data.to_csv(Website_Name+'_'+Data_Pull_time+'.csv',index=False,mode='a')
        self.s3pushdata(Website_Name,Data_Pull_time)

    def s3pushdata(self,Website_Name,Data_Pull_time):
        directory = os.getcwd()
        files=glob.glob(directory+"\*"+Website_Name+'_'+Data_Pull_time+".csv")
        for file in files:
            s3_client = boto3.client('s3')
            s3_client.upload_file(
                Filename=file,
                Bucket="product-data-sm",
                Key="structured-data/shopify-data/" + file.split("\\")[-1])
            print(Website_Name+" file pushed to S3")

shopify_list=['https://rothys.com/','https://ca.endy.com/','https://triangl.com/']

for website in shopify_list:
    products=ShopifyScraper(website)
    products.mains()
