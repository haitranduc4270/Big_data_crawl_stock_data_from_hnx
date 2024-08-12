import requests
import json
from datetime import datetime, timedelta
from constant.constant import date_format
from os import path

# Hàm lấy thông tin các doanh nghiệp và ghi ra data/stock.json

headers = {
        "user-agent":
          "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
      }
    
def get_company_info():
    print('Get company info start')

    # Đọc các thông tin doanh nghiệp có sẵn
    stock_info_front = {}

    if path.exists('data/stock.json'):
        with open('data/stock.json', 'r') as openfile:
            stock_info_front = json.load(openfile)
            stock_info_front = stock_info_front['data']

    print('Done read')

    # Lấy thông tin tất cả doanh nghiệp trên sàn hnx
    result = requests.get('https://iboard-query.ssi.com.vn/v2/stock/group/HNX30', headers = headers)

    hnx = json.loads(result.text)['data']

    # Lấy thông tin tất cả doanh nghiệp trên sàn hose

    result = requests.get('https://iboard-query.ssi.com.vn/v2/stock/group/hose', headers = headers)

    hose = json.loads(result.text)['data']

    stock_info = {
        'time_stamp': (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d') + 'T23:59:59',
        'data': {}
    }

    # Với mỗi doanh nghiệp gọi api lấy thông tin chi tiết
    for stock in hnx:
        info = requests.get('https://iboard-api.ssi.com.vn/statistics/company/company-profile?symbol=' + stock['ss'] + '&language=vn', headers = headers)
        stock_info['data'][stock['ss'] + '-' + 'hnx'] = {}
        stock_info['data'][stock['ss'] + '-' +
                           'hnx']['companyProfile'] = json.loads(info.text)['data']
        stock_info['data'][stock['ss'] + '-' +
                           'hnx']['companyStatistics'] = json.loads(info.text)['data']
    

    # Với mỗi doanh nghiệp gọi api lấy thông tin chi tiết
    for stock in hose:
        info = requests.get('https://iboard-api.ssi.com.vn/statistics/company/company-profile?symbol=' + stock['ss'] + '&language=vn', headers = headers)
        stock_info['data'][stock['ss'] + '-' + 'hose'] = {}
        stock_info['data'][stock['ss'] + '-' +
                           'hose']['companyProfile'] = json.loads(info.text)['data']
        stock_info['data'][stock['ss'] + '-' +
                           'hose']['companyStatistics'] = json.loads(info.text)['data']
    
    # Kiểm tra lại để tránh ghi 1 doanh nghiệp nhiều lần
    for key in stock_info_front:
        if key not in stock_info['data']:
            stock_info['data'][key] = stock_info_front[key]

    json_object = json.dumps(stock_info, indent=4)

    # Ghi dữ liệu ra file tương ứng
    with open('data/stock.json', 'w') as outfile:
        outfile.write(json_object)

    print('Get company info finish')


get_company_info()