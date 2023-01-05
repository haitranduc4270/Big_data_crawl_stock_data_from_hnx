import time
import json
import random
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

def json_serializer(data):
  return json.dumps(data).encode('utf-8')

def start_kafka_producer (bootstrap_servers):
    producer = KafkaProducer(
        bootstrap_servers = bootstrap_servers,
        value_serializer = json_serializer
    )
    return producer
