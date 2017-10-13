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
        self.collection = None
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

        self.collection = db_connection[self.home_team.game_hashtag + self.away_team.game_hashtag]

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

    def get_team_by_names(self, text):

        home_team_occurrences = [x for x in self.home_team.names if x.lower() in text.lower()]
        away_team_occurrences = [x for x in self.away_team.names if x.lower() in text.lower()]

        if len(home_team_occurrences) != 0 and len(away_team_occurrences) == 0:
            return self.home_team
        elif len(home_team_occurrences) == 0 and len(away_team_occurrences) != 0:
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

    def get_team_by_players(self, words):

        home_team_players = [x for x in self.home_team.players if x.lower() in words]
        away_team_players = [x for x in self.away_team.players if x.lower() in words]

        if len(home_team_players) != 0 and len(away_team_players) == 0:
            return self.home_team
        elif len(home_team_players) == 0 and len(away_team_players) != 0:
            return self.away_team
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

        # Try to extract the tweeted team from the names used for both teams
        tweeted_team = self.get_team_by_names(text)

        # Add the polarity to the json
        json_body['polarity'] = polarity

        # If we managed to extract a team, save the tweet along with the name of the team
        if tweeted_team is not None:
            json_body['team'] = tweeted_team.name
            self.collection.insert_one(json_body)
        # If not, try to extract the tweeted team based on the teams' hashtags
        else:
            tweeted_team = self.get_team_by_hashtags([x.lower() for x in hashtags])

            if tweeted_team is not None:
                json_body['team'] = tweeted_team.name
                self.collection.insert_one(json_body)
            else:
                tweeted_team = self.get_team_by_players(text_blob.lower().words)

                if tweeted_team is not None:
                    json_body['team'] = tweeted_team.name
                    self.collection.insert_one(json_body)


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
