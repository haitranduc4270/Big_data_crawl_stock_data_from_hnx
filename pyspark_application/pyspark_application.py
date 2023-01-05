import random
import time
from datetime import datetime

from dependencies import spark
from dependencies import kafka_consumer
from services import service

spark_sess = spark.start_spark()
kafka_consumer_instance = kafka_consumer.start_kafka_consumer('kafka:9092', ['consumer_topic'])

while 1:
  for msg in kafka_consumer_instance:
    service.start(msg, spark_sess)
