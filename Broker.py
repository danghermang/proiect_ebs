#!/usr/bin/env python3
import json
import uuid
import pika

from shared import brokers_exchange, subscriptions_exchange, \
    publications_exchange, match_subscription, parameters

subscribers = []
publications = []
broker_id = str(uuid.uuid4())

print("Broker with Id=%r" % broker_id)

connection = pika.BlockingConnection(parameters)
brokers_channel = connection.channel()
subscriptions_channel = connection.channel()
publications_channel = connection.channel()

# Register broker
brokers_channel.exchange_declare(exchange=brokers_exchange, exchange_type='fanout')
brokers_result = brokers_channel.queue_declare(queue='', exclusive=True)
brokers_result_queue = brokers_result.method.queue
brokers_props = pika.BasicProperties(app_id=broker_id, reply_to=brokers_result_queue)

brokers_channel.basic_publish(exchange=brokers_exchange, routing_key='', properties=brokers_props, body="broker")
print(" Broker with id %r registered" % broker_id)

# Manage subscriptions
subscriptions_channel.exchange_declare(exchange=subscriptions_exchange, exchange_type='fanout')
subscriptions_result = subscriptions_channel.queue_declare(queue='', exclusive=True)
subscriptions_result_queue = subscriptions_result.method.queue
subscriptions_channel.queue_bind(exchange=subscriptions_exchange, queue=subscriptions_result_queue)


def manage_subscriptions(ch, method, properties, body):
    subscriber_id = properties.app_id
    subscriber_queue_name = properties.reply_to
    subscription = [json.loads(body)]
    subscribers.append((subscription[0], subscriber_id, subscriber_queue_name))
    print("Received subscription '{0}' from subscriber with id '{1}'".format(subscription, properties.app_id))
    matched = match_subscription(publications, subscription[0])
    if len(matched) > 0:
        subscriptions_channel.basic_publish(exchange='', routing_key=subscriber_queue_name, properties=properties,
                                            body=json.dumps((matched, subscription[0])))
    print(len(publications))


subscriptions_channel.basic_consume(queue=brokers_result_queue, on_message_callback=manage_subscriptions, auto_ack=True)

# Manage publications
publications_channel.exchange_declare(exchange=publications_exchange, exchange_type='fanout')
publications_result = publications_channel.queue_declare(queue='', exclusive=True)
publications_result_queue = publications_result.method.queue
publications_channel.queue_bind(exchange=publications_exchange, queue=publications_result_queue)


def manage_publications(ch, method, properties, body):
    print("Received publication '{0}' from publisher with id '{1}'".format(json.loads(body), properties.app_id))
    pub = [json.loads(body)]
    for subscriber in subscribers:
        matched = match_subscription(pub, subscriber[0])
        if len(matched) > 0:
            subscriptions_channel.basic_publish(exchange='', routing_key=subscriber[2], properties=properties,
                                                body=json.dumps((matched, subscriber[0])))
    publications.append(pub[0])


publications_channel.basic_qos(prefetch_count=1)
publications_channel.basic_consume(queue=publications_result_queue, on_message_callback=manage_publications,
                                   auto_ack=True)
publications_channel.start_consuming()
subscriptions_channel.start_consuming()
