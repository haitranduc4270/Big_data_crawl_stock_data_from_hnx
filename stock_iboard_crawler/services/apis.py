import requests
import json
from datetime import datetime, timedelta
from constant.constant import time_zone, time_format

# Hàm lấy dữ liệu chứng khoán hiện tại từ api của ssi


def get_stock_real_times_by_group(url, body):
    try:
        result = requests.post(url, json={
            'operationName': body['operationName'],
            'query': body['query'],
            'variables': {
                'exchange': body['variables']['exchange']
            }
        })

        if result.status_code == 200:
            return {
                'timestamp': datetime.now() + timedelta(hours=time_zone),
                'data': json.loads(result.text)['data']['stockRealtimes']
            }

        print('Request stock_real_times_by_group fail' +
              (datetime.now() + timedelta(hours=time_zone)).strftime(time_format))

    except Exception as e:
        print(e)
