import tweepy
import sys
from http.client import IncompleteRead
from requests.packages.urllib3.exceptions import ProtocolError
from streamer.StreamListener import MyStreamListener
from configparser import ConfigParser
from streamer.rabbitmq import RabbitMQ
from threading import Thread


def twitter_filter(stream, tweets):
    while True:
        try:
            stream.filter(track=tweets)
        except IncompleteRead:
            print('Incomplete Read')
        except ProtocolError:
            print('Protocol Error')

if __name__ == '__main__':
    home_team = input('Home Team: ').split()
    away_team = input('Away Team: ').split()

    config = ConfigParser()
    config.read('config.ini')
    # twitter_topics = input('Please enter topics to stream separated with space: ').split()
    # twitter_topics = ['#AFCB', '#Arsenal', 'AVFC', '#CFC', '#COYS', '#CPFC', '#EFC', '#LFC', '#MCFC', '#MUFC', '#WBA']
    auth = tweepy.OAuthHandler(config.get('twitter', 'consumer_token'), config.get('twitter', 'consumer_secret'))
    auth.set_access_token(config.get('twitter', 'access_token'), config.get('twitter', 'access_token_secret'))
    api = tweepy.API(auth)

    home_team_listener = MyStreamListener()
    away_team_listener = MyStreamListener()
    rabbitmq = RabbitMQ(
        config.get('pika', 'rabbitmq_user'),
        config.get('pika', 'rabbitmq_pw'),
        config.get('pika', 'rabbitmq_host')
    )

    home_team_listener.set_queue(rabbitmq, ' '.join(home_team))
    away_team_listener.set_queue(rabbitmq, ' '.join(away_team))
    # twitter_topics = ['#' + param[0][:3] + param[1][:3]]
    home_team_tweets = [' '.join(home_team)]
    away_team_tweets = [' '.join(away_team)]
    home_team_stream = tweepy.Stream(auth=api.auth, listener=home_team_listener)
    away_team_stream = tweepy.Stream(auth=api.auth, listener=away_team_listener)

    home_team_thread = Thread(target=twitter_filter, args=[home_team_stream, home_team_tweets])
    home_team_thread.start()

    away_team_thread = Thread(target=twitter_filter, args=[away_team_stream, away_team_tweets])
    away_team_thread.start()

