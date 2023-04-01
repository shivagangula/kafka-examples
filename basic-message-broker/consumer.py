from kafka import KafkaConsumer
from json import loads
import requests 

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8'))
     )

for message in consumer:
    url = message.value.get("url")
    print(requests.get(url).json().get("brand") )
