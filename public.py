import json
import random
import time

import names

with open("config.json", 'r') as f:
    config = json.load(f)
print config


def randomDate(start="1/1/1971", end="1/1/2009", default=None):
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


functions = {'birth_date': randomDate, 'eye_color': get_eye_color, 'heart_rate': get_heart_rate,
             'name': get_random_name, 'height': get_height}


def generate_publications(config):
    publications = []
    for _ in range(config['number_of_publications']):
        publication = {}
        for probability in config['probabilities']:
            publication[probability] = functions[probability]()
        publications.append(publication)
    return publications


def generate_subscriptions(config):
    subscriptions = []
    for _ in range(config['number_of_subscriptions']):
        subscriptions.append({})
    for pacient_info in config['probabilities']:
        indexes = random_sample(config['number_of_subscriptions'], config['probabilities'][pacient_info])
        if pacient_info == "name":
            count = 0
            operator,operator_probability= config['operator_probabilities']['name']
            number_of_operators = int((operator_probability*len(indexes))/100)
        for index in indexes:
            result = functions[pacient_info](default=True)
            if pacient_info == "name":
                result = functions[pacient_info](default=None)
                if count < number_of_operators:
                    operator_choice = operator
                else:
                    if operator == "=":
                        operator_choice="!="
                    else:
                        operator = "="
                count+=1
                subscriptions[index][pacient_info] = operator_choice, result
            else:
                subscriptions[index][pacient_info] = result
    for element in [x for x in subscriptions if len(x) == 0]:
        to_move = sorted([x for x in subscriptions if len(x) >= 2], reverse=True)[0]
        choice = random.choice(to_move.keys())
        element[choice] = to_move[choice]
        del to_move[choice]
    return subscriptions


with open("publication.json", 'w') as f:
    json.dump(generate_publications(config), f)
with open("subscriptions.json", 'w') as f:
    json.dump(generate_subscriptions(config), f)
# print random_sample(100, 3)
# print randomDate()
# print get_random_name()
# print get_heart_rate()
# print get_eye_color()
# print get_height()
