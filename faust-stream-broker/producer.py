from time import sleep
from json import dumps
from kafka import KafkaProducer
from random import *

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


for e in range(1000):
    apis_list = [
        f"https://retoolapi.dev/9t2uUu/data?id={randint(1, 10)}",
        f"https://retoolapi.dev/PCMKcs/data?id={randint(1, 10)}",
        f"https://retoolapi.dev/s6Q6BF/data?id={randint(1, 10)}",
        f"https://retoolapi.dev/31glvd/data?id={randint(1, 10)}"
    ]
    data = {"transaction_id": "vict891", "valid_vmep_url":choice(apis_list)}
    future =producer.send('consortium_topic', value=data)
    result = future.get(timeout=60)
    #sleep(0.1)

    