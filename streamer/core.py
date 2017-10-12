import tweepy
import sys
from http.client import IncompleteRead
from requests.packages.urllib3.exceptions import ProtocolError
from streamer.StreamListener import MyStreamListener
from configparser import ConfigParser
from streamer.rabbitmq import RabbitMQ


if __name__ == '__main__':
    param = input('Teams: ').split()
    if len(param) != 2:
        print('Two teams should be playing.')
        sys.exit(0)
    config = ConfigParser()
    config.read('config.ini')
    # twitter_topics = input('Please enter topics to stream separated with space: ').split()
    # twitter_topics = ['#AFCB', '#Arsenal', 'AVFC', '#CFC', '#COYS', '#CPFC', '#EFC', '#LFC', '#MCFC', '#MUFC', '#WBA']
    auth = tweepy.OAuthHandler(config.get('twitter', 'consumer_token'), config.get('twitter', 'consumer_secret'))
    auth.set_access_token(config.get('twitter', 'access_token'), config.get('twitter', 'access_token_secret'))
    api = tweepy.API(auth)

    my_stream_listener = MyStreamListener()
    rabbitmq = RabbitMQ(
        config.get('pika', 'rabbitmq_user'),
        config.get('pika', 'rabbitmq_pw'),
        config.get('pika', 'rabbitmq_host')
    )
    my_stream_listener.set_queue(rabbitmq, ''.join(param))
    # twitter_topics = ['#' + param[0][:3] + param[1][:3]]
    twitter_topics = ['Donald Trump']
    my_stream = tweepy.Stream(auth=api.auth, listener=my_stream_listener)
    while True:
        try:
            my_stream.filter(track=twitter_topics)
        except IncompleteRead:
            print('Incomplete Read')
        except ProtocolError:
            print('Protocol Error')
