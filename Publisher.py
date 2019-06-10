#!/usr/bin/env python3
import json
import time
import uuid

import pika

from shared import host, publications_exchange, generate_publications

publisher_id = str(uuid.uuid4())
props = pika.BasicProperties(app_id=publisher_id, delivery_mode=2)

print("Publisher with id=%r start to send publications" % publisher_id)

connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
channel = connection.channel()

channel.exchange_declare(exchange=publications_exchange, exchange_type='fanout')

publications = generate_publications()

for publication in publications:
    channel.basic_publish(exchange=publications_exchange, routing_key='', properties=props,
                          body=json.dumps(publication))
    print("Sent publication %r" % publication)
    time.sleep(2)


print("Publications sent!")
