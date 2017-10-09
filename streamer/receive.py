#!/usr/bin/env python
import pika
from pymongo import MongoClient
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')
db_url = config.get('db', 'db_url')
db = config.get('db', 'db')
collection = config.get('db', 'collection')

rabbitmq_credentials = pika.PlainCredentials(config.get('pika', 'rabbitmq_user'), config.get('pika', 'rabbitmq_pw'))
rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.get('pika', 'rabbitmq_host'),
                                                                        credentials=rabbitmq_credentials))
channel = rabbitmq_connection.channel()
channel.queue_declare(queue='tweet')


def callback(ch, method, properties, body):
    # print(" [x] Received %r" % body)
    text = body.decode('utf-8')
    if 'RT' not in text:
        tweet = {'text': text}
        client = MongoClient('mongodb://127.0.0.1:27017/')
        db_con = client['tweet']
        coll = db_con['tweets']
        object_id = coll.insert_one(tweet).inserted_id
        print('{}: {}'.format(object_id, text))
        client.close()


channel.basic_consume(callback,
                      queue='tweet',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')

channel.start_consuming()
