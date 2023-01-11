from services import service
from dependencies import kafka_consumer
from dependencies import spark
from constant.constant import kafka_bootstrap_servers, kafka_topic
# import time
# from datetime import datetime

# print('Pyspark application stared')

# while True:
#     print(datetime.now())
#     time.sleep(60)


def start_app():
    spark_sess = spark.start_spark()

    kafka_consumer_instance = (kafka_consumer
                               .start_kafka_consumer(kafka_bootstrap_servers, kafka_topic))

    while 1:
        for msg in kafka_consumer_instance:
            service.start(msg, spark_sess)


if __name__ == '__main__':
    try:
        print('Pyspark application stared')
        start_app()
    except Exception as e:
        print(e)
