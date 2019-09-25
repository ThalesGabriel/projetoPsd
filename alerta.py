from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import loads
import json
from time import sleep
from datetime import datetime
import lorem
import time

consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda v: str(v).encode('utf-8'))

consumer.subscribe(pattern='.+')
for message in consumer:
    messages = message.value
    topico = message.topic
    if topico != "alerta":
        try:
            producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: str(v).encode('utf-8'))
            # print(topico)
            new_message = messages.decode("utf-8")
            # print(new_message)
            valor = str(new_message).split(',')[1][0:-1]
            # valor = int(valor)
            # valor = int(valor.replace("'", '').replace('"', "").replace("]", ""))
            valores = topico + "," + valor
            producer.send('alerta', valores)
            print(valores)
        except IndexError:
            print('Ta errado')


    """ print('received: ' + str(message)) """
