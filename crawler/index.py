import time
import json
import random
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "default_topic"

def get_register():
    return {
        'data_dir': 'hdfs://namenode:9000/project20221/raw/ssiDataApi.csv',
        'config_dir': 'hdfs://namenode:9000/project20221/config/stock_data_ssi',
        'topic': KAFKA_TOPIC
    }

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers = ['kafka:9092'], # server name
    value_serializer = json_serializer # function callable
    )

while True:
    user = get_register()
    print(user)
    producer.send(
        'default_topic',user
    )
    time.sleep(6)