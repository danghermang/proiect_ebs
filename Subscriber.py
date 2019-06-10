#!/usr/bin/env python3
import logging
import pickle
import uuid
import pika
import os

from shared import subscriptions_exchange, host, generate_subscriptions,parameters

if not os.path.exists("./logs/"):
    os.makedirs("./logs")
logging.getLogger(__file__)
logging.basicConfig(filename=os.path.join("./logs/",os.path.basename(__file__)+'.log'), level=logging.INFO)
subscriber_id = str(uuid.uuid4())
print("Subscriber with Id=%r start to send subscriptions" % subscriber_id)
logging.info("Subscriber with Id=%r start to send subscriptions" % subscriber_id)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.exchange_declare(exchange=subscriptions_exchange, exchange_type='fanout')
result = channel.queue_declare(queue='', exclusive=True)
result_queue = result.method.queue

props = pika.BasicProperties(app_id=subscriber_id, reply_to=result_queue)

subscriptions = generate_subscriptions()
for subscription in subscriptions:
    channel.basic_publish(exchange=subscriptions_exchange, routing_key='', properties=props,
                          body=pickle.dumps(subscription))
    print("Sent subscription %r" % subscription)
    logging.info("Sent subscription %r" % subscription)


def callback(ch, method, properties, body):
    response = pickle.loads(body)
    print("Received matching publication %r " % response[0])
    logging.info("Received matching publication %r " % response[0])
    print("\t for subscription %r " % response[1])
    logging.info("\t for subscription %r " % response[1])


channel.basic_consume(queue=result_queue, on_message_callback=callback, auto_ack=True)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
