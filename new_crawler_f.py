#!/usr/bin/env python3
# coding: utf-8


import pandas as pd
import numpy as np

import pycountry
import json
from tqdm import tqdm as tqdm
from sklearn.model_selection import ParameterGrid
import geopandas as gpd
import requests
import pycountry
import time
import multiprocessing as mp

invelid_list=['CU','SD','SY','IR','KP','RU']

fileName='femeninos.pkl'
shp_path = "../World_Countries_(Generalized)/World_Countries__Generalized_.shp"

with open('FB.cnf') as f:
    lines = f.readlines()

cred_lst=[[j for j in i if j!=''] for i in [i.replace('\n', '').split(',') for i in lines if i != '\n']]
cred_lst=[i for i in cred_lst if len(i)==3]

# def int_list_getter(cookies,act,token):
    
#     url='https://graph.facebook.com/v13.0/act_'+act+'/targetingbrowse?access_token='+token
#     header={
#     'cookie':cookies,
#     'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
#         }
#     r=json.loads(requests.get(url,headers=header).content)['data']

#     inter_dict={}
#     for i in r:
#         if i['type']=='interests':
#             inter_dict[i['id']]=[i['name'],i['audience_size'],i['path']]
#     return inter_dict

age_category=['0']
gender_categorys=['male','female','0']
ptype=["CPM","CPC"]

# inter_dict=list(int_list_getter(cred_lst[0][2],cred_lst[0][0],cred_lst[0][1]).keys())
inter_dict=[str(x) for x in list(pd.read_csv('femeninos.csv',header=None)[0])]
inter_dict.append('0')

def country_handler(x):
    try :
        return pycountry.countries.lookup(x).alpha_2
    except:
        return np.nan


def valer(nan_lst,age,ctr,ptype,gender,interest,cookies,act,token):
    ptyper={
        'CPM':['IMPRESSIONS','impressions'],
        'CPC':['LINK_CLICKS','actions']
    }
    gender_cart={
        'male':'1',
        'female':'2'
    }
    ages_chart={
        '0':['0','0'],
        'adolescence':['16','19'],
        'early_adulthood':['20','39'],
        'adulthood':['40','64'],
        'maturity' : ['65','0']
        } 
    age_part= '' if age=='0' else ',"age_min":'+ages_chart[age][0]+('' if age=='maturity'  else ',"age_max":'+ages_chart[age][1]) 

    genderPart= '' if gender=='0' else ',"genders":['+gender_cart[gender]+']'
    interest_part='}' if interest=='0' else ',"flexible_spec":[{"interests":[{"id":"'+interest+'"}]}]}'
    header={
    'cookie':cookies,
    'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
        }
    body={
    'currency':'EUR',
    'method':'get',
    'optimization_goal': ptyper[ptype][0],
    'targeting_spec':'{"geo_locations":{"countries":["'+ctr+'"],"location_types":["home"]}'+genderPart+age_part+interest_part

    }
    
    url='https://graph.facebook.com/v13.0/act_'+act+'/delivery_estimate?access_token='+token
    try:
        rm=json.loads(requests.post(url,headers=header, json=body).content)
        r=rm["data"][0]["daily_outcomes_curve"]
        return [r[len(r)-1]['spend']*1000/r[len(r)-1][ptyper[ptype][1]],rm['data'][0]['estimate_mau']] if r[len(r)-1][ptyper[ptype][1]]!=0 else [0,rm['data'][0]['estimate_mau']]
    except:
        print('\n',age,interest,ctr,ptype,gender)
        nan_lst.append([age,interest,ctr,ptype,gender])
        return np.nan


map_df = gpd.read_file(shp_path,na_values=[''])
tqdm.pandas()
nan_lst=[]
try :
    df_FGG=pd.read_pickle(fileName)
    df_FGG=df_FGG.drop(['randNumCol'],axis=1)
    df_FGG['randNumCol'] = np.random.randint(0, len(cred_lst), df_FGG.shape[0])
    df_FGG['randNumCol'] = df_FGG['randNumCol'].apply(lambda x:cred_lst[x])

except :
    grid = {'country': map_df['ISO'] ,'age':age_category,'gender':gender_categorys,'ptype':ptype,'interests':inter_dict}
    rows=[]
    for grid in tqdm(ParameterGrid(grid)):
        country=grid['country']
        age=grid['age']
        gender=grid['gender']
        ptype=grid['ptype']
        interest=grid['interests']
        rows.append([country,age,gender,ptype,interest])
    df_FGG=pd.DataFrame(rows,columns=['country','age','gender','ptype','interest'])
    df_FGG=df_FGG[~df_FGG['country'].isin(invelid_list)]
    df_FGG['randNumCol'] = np.random.randint(0, len(cred_lst), df_FGG.shape[0])
    df_FGG['randNumCol'] = df_FGG['randNumCol'].apply(lambda x:cred_lst[x])
    
    df_FGG['price_mau']=df_FGG[:2].progress_apply(lambda x:valer(nan_lst,x['age'],x['country'],x['ptype'],x['gender'],x['interest'],x['randNumCol'][2],x['randNumCol'][0], x['randNumCol'][1]),axis=1)


def f_app(df):
    return df.progress_apply(lambda x: x['price_mau'] if type(x['price_mau'])==list else valer(nan_lst,x['age'],x['country'],x['ptype'],x['gender'],x['interest'],x['randNumCol'][2],x['randNumCol'][0],x['randNumCol'][1]),axis=1)

nas=sum(df_FGG['price_mau'].isnull()) > 100

print('number of NAs')
print(sum(df_FGG['price_mau'].isnull()))
# import math
while (nas):
        dfs = np.array_split(df_FGG, mp.cpu_count())
        # with mp.Pool(math.floor(mp.cpu_count()*5/6)) as pool:
        with mp.Pool(mp.cpu_count()) as pool:
                df_FGG['price_mau']=pd.concat(pool.map(f_app, dfs))
        df_FGG.to_pickle(fileName)
        nas=sum(df_FGG['price_mau'].isna()) > 100
        if not (nas):
                break
        time.sleep(1200000)
