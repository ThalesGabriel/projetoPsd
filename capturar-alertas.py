from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'alerta',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda v: str(v).encode('utf-8'))

for message in consumer:
    messages = message.value
    new_message = messages.decode("utf-8")[2:-1]
    print(new_message)
    new_message = new_message.split(",")
    new_message[0] = new_message[0].split(".")
    topico = new_message[0][1]
    cidade = new_message[0][0]
    valor = new_message[1]
    print(valor)

    