#!/usr/bin/env python3
import pika

from shared import brokers_exchange, subscriptions_exchange, host

brokers = []
actual_broker = 0
print("Broker overlay management")
connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
brokers_channel = connection.channel()
subscriptions_channel = connection.channel()
publications_channel = connection.channel()

# Manage brokers
brokers_channel.exchange_declare(exchange=brokers_exchange, exchange_type='fanout')
brokers_result = brokers_channel.queue_declare(queue='', exclusive=True)
brokers_result_queue = brokers_result.method.queue
brokers_channel.queue_bind(exchange=brokers_exchange, queue=brokers_result_queue)


def manage_brokers(ch, method, properties, body):
    broker_id = properties.app_id
    broker_queue_name = properties.reply_to
    brokers.append((broker_id, broker_queue_name))
    print("\rRegistered broker with id %r" % broker_id)


brokers_channel.basic_consume(queue=brokers_result_queue, on_message_callback=manage_brokers)

# Manage subscriptions
subscriptions_channel.exchange_declare(exchange=subscriptions_exchange, exchange_type='fanout')
subscriptions_result = subscriptions_channel.queue_declare(queue='', exclusive=True)
subscriptions_result_queue = subscriptions_result.method.queue
subscriptions_channel.queue_bind(exchange=subscriptions_exchange, queue=subscriptions_result_queue)


def get_broker():
    global actual_broker
    actual_broker += 1
    if actual_broker > len(brokers):
        actual_broker = 0
    return actual_broker - 1


def manage_subscriptions(ch, method, properties, body):
    if len(brokers) > 0:
        broker_id = get_broker()
        subscriptions_channel.basic_publish(exchange='', routing_key=brokers[broker_id][1], properties=properties,
                                            body=body)
        print("\rSubscription from '{0}' sent to '{1}'".format(properties.app_id, brokers[broker_id][0]))


subscriptions_channel.basic_consume(queue=subscriptions_result_queue, on_message_callback=manage_subscriptions,
                                    auto_ack=True)
subscriptions_channel.start_consuming()
