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


# Function that validates that the given name is a Premier League team
def valid_team(team):
    return len(team) > 0

if __name__ == '__main__':

    # Read in the name of the teams and make sure they are valid
    home_team = input('Home Team: ')
    while not valid_team(home_team):
        home_team = input('No such team exists. Try again: ')
    away_team = input('Away Team: ')
    while not valid_team(away_team):
        away_team = input('No such team exists. Try again: ')

    config = ConfigParser()
    config.read('config.ini')

    # Set the necessary authentication tokens to be able to stream from Twitter
    auth = tweepy.OAuthHandler(config.get('twitter', 'consumer_token'), config.get('twitter', 'consumer_secret'))
    auth.set_access_token(config.get('twitter', 'access_token'), config.get('twitter', 'access_token_secret'))
    api = tweepy.API(auth)

    # Create a StreamListener to gather tweets for both teams
    home_team_listener = MyStreamListener()
    away_team_listener = MyStreamListener()
    rabbitMQ = RabbitMQ(
        config.get('pika', 'rabbitmq_user'),
        config.get('pika', 'rabbitmq_pw'),
        config.get('pika', 'rabbitmq_host')
    )

    # Initialize a RabbitMQ queue for each stream listener
    home_team_listener.set_queue(rabbitMQ, home_team)
    away_team_listener.set_queue(rabbitMQ, away_team)

    # Set the topics we want to extract for each team
    home_team_tweets = [home_team]
    away_team_tweets = [away_team]

    # Create the stream
    home_team_stream = tweepy.Stream(auth=api.auth, listener=home_team_listener)
    away_team_stream = tweepy.Stream(auth=api.auth, listener=away_team_listener)

    # Separate threads for each streams so we can run them concurrently
    home_team_thread = Thread(target=twitter_filter, args=[home_team_stream, home_team_tweets])
    away_team_thread = Thread(target=twitter_filter, args=[away_team_stream, away_team_tweets])

    # Start the threads
    home_team_thread.start()
    away_team_thread.start()

