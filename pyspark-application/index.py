import time
import json
import random
import logging

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError

consumer = KafkaConsumer(bootstrap_servers='kafka:9092')
consumer.subscribe(['default_topic'])

def get_register():
    return {
        'data_dir': 'hdfs://namenode:9000/project20221/raw/ssiDataApi.csv',
        'config_dir': 'hdfs://namenode:9000/project20221/config/stock_data_ssi',
        'topic': 'consumer_topic'
    }

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers = ['kafka:9092'], # server name
    value_serializer = json_serializer # function callable
    )

while 1:
  for msg in consumer:
    print (json.loads(msg.value.decode('utf-8')))

    user = get_register()
    print(user)
    producer.send(
        'consumer_topic',user
    )
    time.sleep(6)