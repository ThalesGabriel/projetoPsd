from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'meu.topico.legal',
     bootstrap_servers=['172.16.205.225:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda v: str(v).encode('utf-8'))

for message in consumer:
    message = message.value
    print('received: ' + str(message))

    