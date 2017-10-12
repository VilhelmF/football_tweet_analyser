import tweepy
import json
from .send import send_tweet


class MyStreamListener(tweepy.StreamListener):

    tweet_counter = 0

    def __init__(self, max_tweets=1000000, *args, **kwargs):
        self.max_tweets = max_tweets
        self.tweet_counter = 0
        self.queue = None
        self.rabbit_mq = None
        super().__init__(*args, **kwargs)

    def on_connect(self):
        self.tweet_counter = 0

    def set_queue(self, rabbit_mq, queue):
        self.rabbit_mq = rabbit_mq
        self.queue = queue

    def on_status(self, status):
        # Only English
        if hasattr(status, 'retweeted_status'):
            return
        if status.author.lang == 'en':
            message = {'author_name': status.author.screen_name,
                       'create_date': str(status.created_at),
                       'text': status.text,
                       'hashtags': status.entities.get('hashtags'),
                       'coordinates:': status.coordinates
                       }
            self.tweet_counter += 1
            # send_tweet('{} : {}'.format(message['author_name'], message['text']))
            str_message = json.dumps(message)
            if self.queue is not None and self.rabbit_mq is not None:
                # print('StreamListener sending message {}'.format(status.text))
                self.rabbit_mq.publish_message(self.queue, str_message)
                # send_tweet(self.queue, str_message)
            # print('{} : {}'.format(message['author_name'], message['text']))
            if self.tweet_counter >= self.max_tweets:
                return False

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False


