from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'numtest1',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     auto_commit_interval_ms=1000,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

CONNECTION_URL = f"mongodb+srv://neerja:neerja@cluster0.gnvzu.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

client = MongoClient(CONNECTION_URL)

collection = client.PcPerformance.PcPerformance

for message in consumer:
    message = message.value
    print(message)
    collection.insert_one(message)
    print('{} added to {}'.format(message, collection))