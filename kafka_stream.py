from time import sleep
from json import dumps
from kafka import KafkaProducer

topic_name = 'KAFKA_TOPIC'
producer = KafkaProducer(bootstrap_servers=[''],value_serializer=lambda x:dumps(x).encode('utf-8'))

for e in range(10):
    data = {'Herish':e}
    print(data)
    producer.send(topic_name,value=data)
    sleep(1)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime

topic_name = 'KAFKA_TOPIC'
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

for e in range(20):
    data = {
        'Herish': e,
        'timestamp': datetime.now().isoformat()
    }
    print(data)
    producer.send(topic_name, value=data)
    sleep(1)

