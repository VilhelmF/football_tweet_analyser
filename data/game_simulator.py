import pika
from pika.exceptions import ConnectionClosed
import csv
import datetime
from datetime import datetime, timedelta
from configparser import ConfigParser
from streamer.rabbitmq import RabbitMQ
import json


def process_tweets(file_name, start_datetime):
    with open(file_name, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        end_datetime = start_datetime + timedelta(hours=2)
        game_tweets = []
        for tweet in reader:
            tweet_datetime = datetime.strptime(tweet['date'], '%Y-%m-%d %H:%M')
            if start_datetime <= tweet_datetime <= end_datetime:
                hashtags = None if tweet['hashtags'] is None else tweet['hashtags'].split(' ')
                message = {'author_name': tweet['username'],
                           'create_date': tweet['date'],
                           'text': tweet['text'],
                           'hashtags': hashtags,
                           'coordinates:': None
                           }
                game_tweets.append(message)
                # print(message)
        return game_tweets


if __name__ == '__main__':
    '''file_name = input('Filename: ')
    queue_name = input('Queue name: ')
    start_time = input('Start time (%Y-%m-%d %H:%M): ')'''
    queue_name = 'LEILIV'
    start_time = '2017-09-23 16:30'
    game_tweets = process_tweets('output/leiliv.csv', datetime.strptime(start_time, '%Y-%m-%d %H:%M'))
    config = ConfigParser()
    config.read('../streamer/config.ini')
    rabbitmq = RabbitMQ(
        config.get('pika', 'rabbitmq_user'),
        config.get('pika', 'rabbitmq_pw'),
        config.get('pika', 'rabbitmq_host')
    )
    while len(game_tweets) > 0:
        tweet = game_tweets.pop()
        str_message = json.dumps(tweet)
        try:
            rabbitmq.publish_message(queue_name, str_message)
        except ConnectionClosed:
            print('Timeout')

