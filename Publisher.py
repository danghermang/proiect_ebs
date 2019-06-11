#!/usr/bin/env python3
import pickle
import uuid
import pika
import logging
import os
from shared import publications_exchange, publications_generator,parameters

if not os.path.exists("./logs/"):
    os.makedirs("./logs")
logging.getLogger(__file__)
logging.basicConfig(filename=os.path.join("./logs/",os.path.basename(__file__)+'.log'), level=logging.INFO)
publisher_id = str(uuid.uuid4())
props = pika.BasicProperties(app_id=publisher_id, delivery_mode=2)

print("Publisher with id=%r start to send publications" % publisher_id)
logging.info("Publisher with id=%r start to send publications" % publisher_id)


connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.exchange_declare(exchange=publications_exchange, exchange_type='fanout')


for publication in publications_generator():
    channel.basic_publish(exchange=publications_exchange, routing_key='', properties=props,
                          body=pickle.dumps(publication))
    print("Sent publication %r" % publication)
    logging.info("Sent publication %r" % publication)


print("Publications sent!")
logging.info("Publications sent!")
input()
connection.close()
