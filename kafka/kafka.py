# -*- coding: utf-8 -*-
from kafka import KafkaProducer
import json

def producer_init():
    return KafkaProducer(
        bootstrap_servers=['192.168.31.242:9092'],
        key_serializer=lambda k: json.dumps(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode())


