from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
consumer.subscribe(['test-topic'])

while 1:
  for msg in consumer:
    print (msg)