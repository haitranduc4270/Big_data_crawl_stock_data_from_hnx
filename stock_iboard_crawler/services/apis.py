import requests
import json
from datetime import datetime, timedelta
from constant.constant import time_zone, time_format

# Hàm lấy dữ liệu chứng khoán hiện tại từ api của ssi


def get_stock_real_times_by_group(work):
    try:

        headers = {
            "user-agent":
                "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        }
        
        result = requests.get(work['source_v2']['url'], headers = headers)

        if result.status_code == 200:
            return {
                'timestamp': datetime.now() + timedelta(hours=time_zone),
                'data': json.loads(result.text)['data']
            }

        print('Request stock_real_times_by_group fail' +
              (datetime.now() + timedelta(hours=time_zone)).strftime(time_format))

    except Exception as e:
        # In ra bất kỳ lỗi chung nào khác
        print(f"An unexpected error occurred: {e}")