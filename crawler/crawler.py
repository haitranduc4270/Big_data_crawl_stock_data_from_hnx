import random
import time
from datetime import datetime

from dependencies import spark
from dependencies import kafka_producer
from apis import get_stock_real_times_by_group

def start_crawler ():
    spark_sess = spark.start_spark()
    kafka_producer_instance = kafka_producer.start_kafka_producer(['kafka:9092'])

    while True:
        print(datetime.now())
        time.sleep(60)

        data_dir = get_stock_real_times_by_group(spark_sess)
        config_dir = '/config/ssi_stock_data.json'

        kafka_producer_instance.send('consumer_topic', {
            'data_dir': data_dir,
            'config_dir': config_dir,
        })


