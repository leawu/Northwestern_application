import re
import requests
import pandas as pd
import json
from datetime import datetime
import concurrent.futures
import time
from random import randint
import pickle
import os


def getNearShop(lat, lng, city, loc):
    url = 'https://disco.deliveryhero.io/listing/api/v1/pandora/vendors'

    result = {}
    result['shopName'] = []
    result['shopCode'] = []
    result['budget'] = []
    result['category'] = []
    result['pandaOnly'] = []
    result['minFee'] = []
    result['minOrder'] = []
    result['minDelTime'] = []
    result['minPickTime'] = []
    result['distance'] = []
    result['rateNum'] = []
    
    query = {
            'longitude': lng,
            'latitude': lat,
            'language_id': 6,
            'include': 'characteristics',
            'dynamic_pricing': 0,
            'configuration': 'Variant1',
            'country': 'tw',
            'budgets': '',
            'cuisine': '',
            'sort': '',
            'food_characteristic': '',
            'use_free_delivery_label': False,
            'vertical': 'restaurants',
            'limit': 48,
            'offset': 0,
            'customer_type': 'regular'
        }
    headers = {
        'x-disco-client-id': 'web',
    }
    r = requests.get(url=url, params=query, headers=headers)

    if r.status_code == requests.codes.ok:
        data = r.json()
        datalen = data['data']['available_count']
        restaurants = data['data']['items']
        for restaurant in restaurants:
            result['shopName'].append(restaurant['name'])
            result['shopCode'].append(restaurant['code'])
            result['budget'].append(restaurant['budget'])
            result['distance'].append(restaurant['distance'])
            result['pandaOnly'].append(restaurant['is_best_in_city'])
            result['rateNum'].append(restaurant['review_number'])
            
            tmp = []
            for cat in restaurant['cuisines']:
                tmp.append(cat['name'])
            result['category'].append(tmp)
            try:
                result['minFee'].append(restaurant['minimum_delivery_fee'])
            except:
                result['minFee'].append("")
            try:
                result['minOrder'].append(restaurant['minimum_order_amount'])
            except:
                result['minOrder'].append("")
            try:
                result['minDelTime'].append(restaurant['minimum_delivery_time'])
            except:
                result['minDelTime'].append("")
            try:
                result['minPickTime'].append(restaurant['minimum_pickup_time'])
            except:
                result['minPickTime'].append("")

    for i in range(1, datalen, 100):
        query = {
            'longitude': lng,
            'latitude': lat,
            'language_id': 6,
            'include': 'characteristics',
            'dynamic_pricing': 0,
            'configuration': 'Variant1',
            'country': 'tw',
            'budgets': '',
            'cuisine': '',
            'sort': '',
            'food_characteristic': '',
            'use_free_delivery_label': False,
            'vertical': 'restaurants',
            'limit': datalen,
            'offset': i,
            'customer_type': 'regular'
        }
        headers = {
            'x-disco-client-id': 'web',
        }
        r = requests.get(url=url, params=query, headers=headers)

        if i%10==0:
            time.sleep(3)
            print('sleeping at' , i)
        elif i%5==0:
            time.sleep(randint(0,4))
            print('sleeping at' , i)

        if r.status_code == requests.codes.ok:
            data = r.json()
            restaurants = data['data']['items']
            for restaurant in restaurants:
                result['shopName'].append(restaurant['name'])
                result['shopCode'].append(restaurant['code'])
                result['budget'].append(restaurant['budget'])
                result['distance'].append(restaurant['distance'])
                result['pandaOnly'].append(restaurant['is_best_in_city'])
                result['rateNum'].append(restaurant['review_number'])
                tmp = []
                for cat in restaurant['cuisines']:
                    tmp.append(cat['name'])
                result['category'].append(tmp)
                try:
                    result['minFee'].append(restaurant['minimum_delivery_fee'])
                except:
                    result['minFee'].append("")
                try:
                    result['minOrder'].append(restaurant['minimum_order_amount'])
                except:
                    result['minOrder'].append("")
                try:
                    result['minDelTime'].append(restaurant['minimum_delivery_time'])
                except:
                    result['minDelTime'].append("")
                try:
                    result['minPickTime'].append(restaurant['minimum_pickup_time'])
                except:
                    result['minPickTime'].append("")
        else:
            print('Erro')
    df = pd.DataFrame.from_dict(result)

    df.to_csv(f'./outputput/shopLst/shopLst_{city}_{loc}_1211.csv')

def concatDF():
    joinedlist = []
    dir_list = os.listdir("../output/shopLst")

    for file in dir_list:
        if file!='.DS_Store':
            tmp = pd.read_csv('../output/shopLst/'+file)
            joinedlist.append(tmp)

    df = pd.concat(joinedlist)
    df = df.dropna()
    df = df.drop_duplicates()
    df.to_csv(f'../output/all_most.csv')

    return df

def getMenu(restaurant_code):
    currentTime = datetime.now()
    result = {}

    url = f'https://tw.fd-api.com/api/v5/vendors/{restaurant_code}'
    query = {
        'include': 'menus',
        'language_id': '6',
        'dynamic_pricing': '0',
        'opening_type': 'delivery',
    }
    
   
    data = requests.get(url=url
        , params=query
        )
    if data.status_code == requests.codes.ok:
        data = data.json()
        result['shopCode'] = restaurant_code
        result['Url'] = url
        result['address'] = data['data']['address']
        result['location'] = [data['data']['latitude'], data['data']['longitude']]
        result['rate'] = data['data']['rating']
        result['updateDate'] = currentTime
        tmp = []
        if data['data']['is_pickup_enabled']==True:
            result['pickup'] = 1
        else:
            result['pickup'] = 0
        tmpInshop = 0
        for i in range(len(data['data']['food_characteristics'])):
            if '店內價' in data['data']['food_characteristics'][i]['name']:
                tmpInshop = 1
            else:
                try:
                    tmp.append(data['data']['food_characteristics'][i]['name'])
                except:
                    pass
        if tmpInshop==1:
            result['inShopPrice'] = 1
        else:
            result['inShopPrice'] = 0

        result['shopTag'] = tmp

        tmp = []
        for i in range(len(data['data']['discounts'])):
            tmp.append(data['data']['discounts'][i]['name'])
        result['discount'] = tmp
        tmp = {}
        tmp['product'] = []
        tmp['preDiscountPrice'] = []
        tmp['discountedPrice'] = []
        try:
            for i in range(len(data['data']['menus'][0]['menu_categories'])):
                for k in range(len(data['data']['menus'][0]['menu_categories'][i]['products'])):
                    tmp['product'].append(data['data']['menus'][0]['menu_categories'][i]['products'][k]['name'])
                    try:
                        tmp['preDiscountPrice'].append(data['data']['menus'][0]['menu_categories'][i]['products'][k]['product_variations'][0]['price_before_discount'])
                    except:
                        tmp['preDiscountPrice'].append('')
                    tmp['discountedPrice'].append(data['data']['menus'][0]['menu_categories'][i]['products'][k]['product_variations'][0]['price'])

        except:
            tmp['product'].append('')
            tmp['preDiscountPrice'].append('')
            tmp['discountedPrice'].append('')
        
        result['menu'] = tmp                 

    else:
        pass
    return result
   
def read_pickle_to_df(fileCnt):
    joinedlist = []
    dir_list = os.listdir("./output/pickleFile")

    for file in dir_list:
        if file!='.DS_Store':
            tmp = pickle.load(open('./output/pickleFile/'+file, 'rb'))
            joinedlist = joinedlist + tmp

    df = pd.DataFrame(joinedlist)
    df = df.dropna()
    df.to_csv(f'../../Foodpanda/foodpandaMenu_{fileCnt}.csv')

shopLst_nearest = pd.read_csv("./input/ttl/all_nearest.csv")
shopLst_most = pd.read_csv("./input/ttl/all_most.csv")

with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
    ttlResult = list(executor.map(getNearShop,  shopLst_most['shopCode'].to_list()))

shopData = concatDF()


for i in range(len(shopData)):
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        ttlResult = list(executor.map(getMenu,  shopLst_nearest['shopCode'].to_list()))

pickle.dump(ttlResult, open('./output/pickleFile/foodpandaMenu_1.pkl', 'wb'))


