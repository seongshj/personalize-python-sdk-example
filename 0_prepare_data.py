import boto3
import pandas as pd
import load_config as conf

personalize = boto3.client("personalize", region_name= conf.app_env['region'])


def load_interactions_data(origin_path, last_path):

    data = pd.read_csv(origin_path, sep='::', names=['USER_ID','ITEM_ID','EVENT_VALUE', 'TIMESTAMP'], engine='python')
    data['EVENT_TYPE']='RATING'
    #data = data[['userId','movieId','rating', 'timestamp']]    data.head(5)
    data.to_csv(last_path, index=False)

def load_items_data(origin_path, last_path):
    data = pd.read_csv(origin_path)
    data = data.drop('title', axis=1)
    data.columns = ['ITEM_ID','GENRE']
    data.to_csv(last_path, index=False)

def load_users_data(origin_path, last_path):
    data = pd.read_csv(origin_path, sep=';')
    data = data.drop('ZipCode', axis=1)
    data.columns = ['USER_ID','GENDER', 'AGE', 'OCCUPATION']
    data.to_csv(last_path, index=False)

##-------------------------------------------

load_interactions_data(conf.app_env['local']['org_interactions'], conf.app_env['local']['interactions'])


load_users_data(conf.app_env['local']['org_users'], conf.app_env['local']['users'])

load_items_data(conf.app_env['local']['org_items'], conf.app_env['local']['items'])


