#!/usr/bin/env python3
import datetime
import json
import random
import time
import names
import pika
import os
import logging

if not os.path.exists("./logs/"):
    os.makedirs("./logs")
if __name__ == "__main__":
    logging.getLogger(__file__)
    logging.basicConfig(filename=os.path.join("./logs/",os.path.basename(__file__)+'.log'), level=logging.INFO)
host = "ebs"
credentials = pika.PlainCredentials('ebs', 'ebs')
parameters = pika.ConnectionParameters('192.168.119.1',
                                       5672,
                                       '/',
                                       credentials)
brokers_exchange = "brokers"
subscriptions_exchange = "subscriptions"
publications_exchange = "publications"

with open("config.json", 'r') as f:
    config = json.load(f)


def random_date(start="1/1/1971", end="1/1/2009", default=None):
    end_to_modify = 0
    start_to_modify = 0
    prop = random.random()
    date_format = '%m/%d/%Y'
    stime = time.mktime(time.strptime(start, date_format)) + start_to_modify
    etime = time.mktime(time.strptime(end, date_format)) - end_to_modify
    ptime = stime + prop * (etime - stime)
    if default:
        choices = ["=", "!=", "<", "<=", ">", ">="]
        return random.choice(choices), time.strftime(date_format, time.localtime(ptime))
    return time.strftime(date_format, time.localtime(ptime))


def random_sample(total_number, percentage):
    percentage = int(percentage * total_number / 100)
    return random.sample(range(total_number), percentage)


def get_random_name(default=None):
    if default:
        global config
        choices = ["=", "!="]
        return random.choice(choices), names.get_full_name()
    return names.get_full_name()


def get_heart_rate(minim=50, maxim=230, default=None):
    if default:
        choices = ["=", "!=", "<", "<=", ">", ">="]
        return random.choice(choices), random.randint(minim, maxim)
    return random.randint(minim, maxim)


def get_height(minim=150, maxim=220, default=None):

    if default:
        choices = ["=", "!=", "<", "<=", ">", ">="]
        return random.choice(choices), random.randint(minim, maxim)
    return random.randint(minim, maxim)


def get_eye_color(default=None):
    eye_colors = ["blue", "green", "brown", "black", "odd-eyed"]
    if default:
        choices = ["=", "!="]
        return random.choice(choices), random.choice(eye_colors)
    return random.choice(eye_colors)


functions = {'birth_date': random_date, 'eye_color': get_eye_color, 'heart_rate': get_heart_rate,
             'name': get_random_name, 'height': get_height}


def generate_publications():
    publications = []
    logging.info("Generating "+str(config['number_of_publications'])+"publications")
    for _ in range(config['number_of_publications']):

        publication = {}
        for probability in config['probabilities']:
            publication[probability] = functions[probability]()
        publications.append(publication)
    return publications


def generate_subscriptions():
    subscriptions = []
    for _ in range(config['number_of_subscriptions']):
        subscriptions.append({})
    logging.info("Generating " + str(config['number_of_subscriptions']) + "subscriptions")
    for pacient_info in config['probabilities']:
        indexes = random_sample(config['number_of_subscriptions'], config['probabilities'][pacient_info])
        if pacient_info == "name":
            count = 0
            operator, operator_probability = config['operator_probabilities']['name']
            number_of_operators = int((operator_probability * len(indexes)) / 100)
        for index in indexes:
            result = functions[pacient_info](default=True)
            if pacient_info == "name":
                result = functions[pacient_info](default=None)
                if count < number_of_operators:
                    operator_choice = operator
                else:
                    if operator == "=":
                        operator_choice = "!="
                    else:
                        operator = "="
                count += 1
                subscriptions[index][pacient_info] = operator_choice, result
            else:
                subscriptions[index][pacient_info] = result
    for element in [x for x in subscriptions if len(x) == 0]:
        to_move = sorted([x for x in subscriptions if len(x) >= 2], reverse=True)[0]
        choice = random.choice(to_move.keys())
        element[choice] = to_move[choice]
        del to_move[choice]
    return subscriptions


def match_by_condition(condition, subscription, publication):
    if condition == "=":
        if subscription != publication:
            return False
    if condition == "!=":
        if subscription == publication:
            return False
    if condition == "<":
        if subscription <= publication:
            return False
    if condition == "<=":
        if subscription < publication:
            return False
    if condition == ">":
        if subscription >= publication:
            return False
    if condition == ">=":
        if subscription > publication:
            return False
    return True


def match_using_filter(publication, subscription):
    if 'name' in subscription:
        if not match_by_condition(list(subscription['name'])[0], list(subscription['name'])[1], publication['name']):
            return False
    if 'eye_color' in subscription:
        if not match_by_condition(list(subscription['eye_color'])[0], list(subscription['eye_color'])[1],
                                  publication['eye_color']):
            return False
    if 'birth_date' in subscription:
        if not match_by_condition(list(subscription['birth_date'])[0],
                                  datetime.datetime.strptime(list(subscription['birth_date'])[1], '%m/%d/%Y'),
                                  datetime.datetime.strptime(publication['birth_date'], '%m/%d/%Y')):
            return False
    if 'heart_rate' in subscription:
        if not match_by_condition(list(subscription['heart_rate'])[0], int(list(subscription['heart_rate'])[1]),
                                  int(publication['heart_rate'])):
            return False
    if 'height' in subscription:
        if not match_by_condition(list(subscription['height'])[0], int(list(subscription['height'])[1]),
                                  int(publication['height'])):
            return False
    return True


def match_subscription(publications, subscription):
    result = [publication for publication in publications if match_using_filter(publication, subscription)]
    return result


'''
pubs = [{'name': 'Celia Wagoner', 'eye_color': 'black', 'birth_date': '08/06/1999', 'heart_rate': 153, 'height': 155},
        {'name': 'Derrick Vincent', 'eye_color': 'black', 'birth_date': '01/14/2002', 'heart_rate': 107, 'height': 190}]
sub = {'name': ['!=', 'William Muhammad'], 'birth_date': ['<', '01/14/2002'], 'heart_rate': ['>', 108]}

print(match_subscription(pubs, sub))'''
