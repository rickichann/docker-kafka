from kafka import KafkaConsumer
import json

bootstrap_servers = ['localhost:29092']
topicName = 'source.public.factinternetsales_streaming'
consumer = KafkaConsumer(topicName, auto_offset_reset = 'earliest', group_id = 'sales-group', bootstrap_servers = bootstrap_servers)

for msg in consumer:
    print(json.loads(msg.value))