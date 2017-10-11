import sys
import urllib.parse

from pymongo import MongoClient
from configparser import ConfigParser
from streamer.rabbitmq import RabbitMQ
from streamer.premier_league import PremierLeague
from textblob import TextBlob
import json


class Analyser:
    def __init__(self, home_team, away_team):
        self.home_team = home_team
        self.away_team = away_team
        self.config = ConfigParser()
        self.db_collection = None
        self.rabbitmq = None
        self.premier_league = None
        self.setup()

    def setup(self):
        self.init_db()
        self.init_rabbitmq()

    def init_db(self):
        self.config.read('config.ini')
        username = urllib.parse.quote_plus(self.config.get('db', 'user'))
        password = urllib.parse.quote_plus(self.config.get('db', 'password'))
        mongo_client = MongoClient('mongodb://%s:%s@' % (username, password) + self.config.get('db', 'db_url'))
        db_connection = mongo_client[self.config.get('db', 'db_name')]
        self.db_collection = db_connection[home_team.replace(' ', '') + away_team.replace(' ', '')]
        print('Database connection to {}.{} established'.format(self.config.get('db', 'db_name'), 'test'))

    def init_rabbitmq(self):
        self.config.read('config.ini')
        self.rabbitmq = RabbitMQ(
            self.config.get('pika', 'rabbitmq_user'),
            self.config.get('pika', 'rabbitmq_pw'),
            self.config.get('pika', 'rabbitmq_host')
        )
        queue = home_team + away_team
        queue = queue.replace(' ', '')
        self.rabbitmq.init_consumer_thread(queue, self.tweet_callback)
        print('Rabbit mq connection to {} : {} established'.format(self.config.get('pika', 'rabbitmq_host'), queue))

    def init_premier_league(self):
        self.premier_league = PremierLeague()

    def tweet_callback(self, ch, method, properties, body):
        json_body = json.loads(body.decode('utf-8'))
        text = json_body['text']
        text_blob = TextBlob(text)
        polarity = text_blob.sentiment.polarity
        json_body['polarity'] = polarity
        if abs(polarity) > 0.2:
            print('{} | Polarity: {}'.format(text, text_blob.sentiment.polarity))
            print('Tags: {}'.format(text_blob.tags))
            print('Nouns: {}'.format(text_blob.noun_phrases))
        print(json_body)
        # object_id = self.db_collection.insert_one(json_body).inserted_id
        # print('{}: {}'.format(object_id, text))

if __name__ == '__main__':
    param = input('Teams: ').split()
    if len(param) != 2:
        print('Two teams should be playing.')
        sys.exit(0)
    home_team, away_team = param
    print('Starting analyser for {} - {}'.format(home_team, away_team))
    analyser = Analyser(home_team, away_team)
