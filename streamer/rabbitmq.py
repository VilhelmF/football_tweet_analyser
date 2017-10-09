import pika
from configparser import ConfigParser
from threading import Thread


class RabbitMQ(object):
    def __init__(self, user, password, host):
        self.config = ConfigParser()
        self.config.read('config.ini')
        self.user = user
        self.password = password
        self.host = host
        self.consumer = None
        self.callback = None
        self.producer = None

    def init_consumer_thread(self, queue, callback):
        credentials = pika.PlainCredentials(self.user, self.password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                                                                       credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue=queue)
        channel.basic_consume(callback,
                              queue=queue,
                              no_ack=True)
        t1 = Thread(target=channel.start_consuming)
        t1.start()
        t1.join(0)

    def publish_message(self, queue, tweet):
        credentials = pika.PlainCredentials(self.user, self.password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                                                                       credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue=queue)
        channel.basic_publish(exchange='',
                              routing_key=queue,
                              body=tweet)
        connection.close()

