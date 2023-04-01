from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

URL = "https://random-data-api.com/api/v2/beers"

for e in range(10):
    data = {'url' : URL, "i": e}
    future =producer.send('numtest', value=data)
    result = future.get(timeout=60)
    #sleep(0.1)