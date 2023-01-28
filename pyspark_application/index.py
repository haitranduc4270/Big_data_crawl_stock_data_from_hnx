
from datetime import datetime, timedelta
from services.company import get_company_info
from services import service
from dependencies import spark, kafka_consumer
import json
from os import path
from constant.constant import kafka_bootstrap_servers, kafka_topic, elasticsearch_time_format
from datetime import datetime

print('Pyspark application stared')

# while True:
#     print(datetime.now())
#     time.sleep(60)


def start_app():
    spark_sess = spark.start_spark()

    kafka_consumer_instance = (kafka_consumer
                               .start_kafka_consumer(kafka_bootstrap_servers, kafka_topic))

    while 1:
        if not path.exists('data/stock.json'):
            get_company_info()
        else:
            with open('data/stock.json', 'r') as openfile:
                stock_info = json.load(openfile)
            if datetime.now() + timedelta(hours=7) > datetime.strptime(
                    stock_info['time_stamp'], elasticsearch_time_format):
                get_company_info()

            else:
                print('Listen to kafka')
                for msg in kafka_consumer_instance:
                    service.start(msg, stock_info['data'], spark_sess)


if __name__ == '__main__':
    try:
        print('Pyspark application stared')
        start_app()
    except Exception as e:
        print(e)
