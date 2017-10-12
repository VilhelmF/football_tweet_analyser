import tweepy
import sys
from http.client import IncompleteRead
from requests.packages.urllib3.exceptions import ProtocolError
from streamer.StreamListener import MyStreamListener
from streamer.utils import valid_team
from streamer.Team import Team
from configparser import ConfigParser

from streamer.rabbitmq import RabbitMQ

if __name__ == '__main__':

    config = ConfigParser()
    config.read('config.ini')

    # Read in the name of the teams and make sure they are valid
    home_team_name = input('Home Team: ')
    while not valid_team(config, home_team_name):
        home_team_name = input('No such team exists. Try again: ')
    away_team_name = input('Away Team: ')
    while not valid_team(config, away_team_name):
        away_team_name = input('No such team exists. Try again: ')

    home_team = Team(home_team_name)
    away_team = Team(away_team_name)

    game_hashtag = home_team.game_hashtag + away_team.game_hashtag

    # Set the necessary authentication tokens to be able to stream from Twitter
    auth = tweepy.OAuthHandler(config.get('twitter', 'consumer_token'), config.get('twitter', 'consumer_secret'))
    auth.set_access_token(config.get('twitter', 'access_token'), config.get('twitter', 'access_token_secret'))
    api = tweepy.API(auth)

    # Create a StreamListener to gather tweets for both teams
    listener = MyStreamListener()
    rabbitMQ = RabbitMQ(
        config.get('pika', 'rabbitmq_user'),
        config.get('pika', 'rabbitmq_pw'),
        config.get('pika', 'rabbitmq_host')
    )

    # Initialize a RabbitMQ queue for each stream listener
    listener.set_queue(rabbitMQ, game_hashtag)

    # Set the topics we want to extract for each team
    tweet_topics = [home_team.name, away_team.name, game_hashtag]

    tweet_topics.extend(home_team.hashtags)
    tweet_topics.extend(away_team.hashtags)

    # Create the stream
    twitter_stream = tweepy.Stream(auth=api.auth, listener=listener)

    while True:
        try:
            twitter_stream.filter(track=tweet_topics)
        except IncompleteRead:
            print('Incomplete Read')
        except ProtocolError:
            print('Protocol Error')


