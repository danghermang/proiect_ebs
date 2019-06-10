#!/usr/bin/env python3
import json
import time
import uuid
import pika

from shared import subscriptions_exchange, host, generate_subscriptions,credentials

subscriber_id = str(uuid.uuid4())
print("Subscriber with Id=%r start to send subscriptions" % subscriber_id)

connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials))
channel = connection.channel()

channel.exchange_declare(exchange=subscriptions_exchange, exchange_type='fanout')
result = channel.queue_declare(queue='', exclusive=True)
result_queue = result.method.queue

props = pika.BasicProperties(app_id=subscriber_id, reply_to=result_queue)

subscriptions = generate_subscriptions()
for subscription in subscriptions:
    channel.basic_publish(exchange=subscriptions_exchange, routing_key='', properties=props,
                          body=json.dumps(subscription))
    print("Sent subscription %r" % subscription)
    time.sleep(2)


def callback(ch, method, properties, body):
    response = json.loads(body)
    print("Received matching publication %r " % response[0])
    print("\t for subscription %r " % response[1])
    print()


channel.basic_consume(queue=result_queue, on_message_callback=callback, auto_ack=True)
channel.start_consuming()
