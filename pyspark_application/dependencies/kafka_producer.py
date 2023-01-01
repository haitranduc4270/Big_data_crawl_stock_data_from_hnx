import time
import json
import random
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

def json_serializer(data):
  return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers = ['kafka:9092'],
    value_serializer = json_serializer
)

class KafkaProducer:
  def __init__(self):
    self.producer = producer

  def send_to_topic(self, kafka_topic, data):
    result = self.producer.send(kafka_topic, data)
    try:
        record_metadata = result.get(timeout=10)
    except KafkaError:
        logging.exception("Error")
        pass
    
    producer.flush()