import sys
import urllib.parse

from pymongo import MongoClient
from configparser import ConfigParser
from streamer.rabbitmq import RabbitMQ
from streamer.premier_league import PremierLeague
from streamer.utils import valid_team
from streamer.Team import Team
from textblob import TextBlob
import json


class Analyser:
    def __init__(self, home_team, away_team):
        self.home_team = Team(home_team)
        self.away_team = Team(away_team)
        self.config = ConfigParser()
        self.rabbitmq = None
        self.premier_league = None
        self.setup()

    def setup(self):
        self.init_db()
        self.init_rabbitmq()

    def init_db(self):
        self.config.read('config.ini')

        # Retrieve the username and password for the database
        username = urllib.parse.quote_plus(self.config.get('db', 'user'))
        password = urllib.parse.quote_plus(self.config.get('db', 'password'))

        # Connect to the database
        mongo_client = MongoClient('mongodb://%s:%s@' % (username, password) + self.config.get('db', 'db_url'))
        db_connection = mongo_client[self.config.get('db', 'db_name')]

        print('Database connection to {}.{} established'.format(self.config.get('db', 'db_name'), 'test'))

    def init_rabbitmq(self):
        self.config.read('config.ini')

        # Initialize the RabbitMQ
        self.rabbitmq = RabbitMQ(
            self.config.get('pika', 'rabbitmq_user'),
            self.config.get('pika', 'rabbitmq_pw'),
            self.config.get('pika', 'rabbitmq_host')
        )

        # Initialize a separate rabbitMQ thread for each team to extract information for the tweets
        self.rabbitmq.init_consumer_thread(self.home_team.game_hashtag + self.away_team.game_hashtag,
                                           self.tweet_analyser)
        print('Rabbit mq connection to {} : {} - {} established'.format(self.config.get('pika', 'rabbitmq_host'),
                                                                        self.home_team.name, self.away_team.name))

    def get_team_by_names(self, nouns):
        if self.home_team.name.lower() in nouns and self.away_team.name.lower() not in nouns:
            return self.home_team
        elif self.home_team.name.lower() not in nouns and self.away_team.name.lower() in nouns:
            return self.away_team
        else:
            return None

    def get_team_by_hashtags(self, hashtags):

        home_team_hashtags = set([x.lower() for x in self.home_team.hashtags])
        away_team_hashtags = set([x.lower() for x in self.away_team.hashtags])

        if len(home_team_hashtags.intersection(set(hashtags))) == 0 and len(away_team_hashtags.intersection(set(hashtags))) != 0:
            return self.away_team
        elif len(away_team_hashtags.intersection(set(hashtags))) == 0 and len(home_team_hashtags.intersection(set(hashtags))) != 0:
            return self.home_team
        else:
            return None

    def init_premier_league(self):
        self.premier_league = PremierLeague()

    def tweet_analyser(self, ch, method, properties, body):

        # Extract the json from the tweet
        json_body = json.loads(body.decode('utf-8'))
        text = json_body['text']
        hashtags = json_body['hashtags']

        # Use TextBlob for sentiment analysis on the tweet and extract the sentiment
        text_blob = TextBlob(text)
        polarity = text_blob.sentiment.polarity

        tweeted_team = self.get_team_by_names([x.lower() for x in text_blob.noun_phrases])

        if tweeted_team is not None:
            # TODO Prediction Magic
            print('')
        else:
            tweeted_team = self.get_team_by_hashtags([x['text'].lower() for x in hashtags])

            if tweeted_team is not None:
                # TODO Prediction Magic
                print('')

        json_body['polarity'] = polarity

        # object_id = self.db_collection.insert_one(json_body).inserted_id
        # print('{}: {}'.format(object_id, text))

if __name__ == '__main__':

    config = ConfigParser()
    config.read('config.ini')

    home_team_name = input('Home Team: ')
    while not valid_team(config, home_team_name):
        home_team_name = input('No such team exists. Try again: ')
    away_team_name = input('Away Team: ')
    while not valid_team(config, away_team_name):
        away_team_name = input('No such team exists. Try again: ')

    print('Starting analyser for {} - {}'.format(home_team_name, away_team_name))
    analyser = Analyser(home_team_name, away_team_name)
