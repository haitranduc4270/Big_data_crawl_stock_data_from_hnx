import json
from kafka import KafkaConsumer

def start_kafka_consumer (bootstrap_servers, topic):
  consumer = KafkaConsumer(bootstrap_servers = bootstrap_servers)
  consumer.subscribe(topic)

  return consumer