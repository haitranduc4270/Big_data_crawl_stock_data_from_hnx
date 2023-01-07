import requests
import json
from datetime import datetime, timedelta
from constant.constant import time_zone, time_format

def get_analys_news_vndirect(url) :
    try:
        result = requests.get(url)
        if result.status_code == 200:

            return {
                'timestamp': datetime.now() + timedelta(hours = time_zone),
                'data': json.loads(result.text)['data']
            }

        print('Request get_analys_news_vndirect fail')

    except Exception as e:
        print(e)

def get_news_vndirect(url) :
    try:
        result = requests.get(url)
        if result.status_code == 200:

            return {
                'timestamp': datetime.now() + timedelta(hours = time_zone),
                'data': json.loads(result.text)['data']
            }

        print('Request get_news_vndirect fail' + (datetime.now() + timedelta(hours = time_zone)).strftime(time_format))

    except Exception as e:
        print(e)


def get_stock_real_times_by_group (url, body):
    try:
        result = requests.post(url, json = {
            'operationName' : body['operationName'],
            'query': body['query'],
            'variables': {
                'group': body['variables']['group']
            }
        })

        if result.status_code == 200:

            return {
                'timestamp': datetime.now() + timedelta(hours = time_zone),
                'data': json.loads(result.text)['data']['stockRealtimesByGroup']
            }

        print('Request stock_real_times_by_group fail' + (datetime.now() + timedelta(hours = time_zone)).strftime(time_format))

    except Exception as e:
        print(e)
