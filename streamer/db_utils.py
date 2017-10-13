from pymongo import MongoClient
from configparser import ConfigParser
import urllib.parse


def create_connection(collection):
    config = ConfigParser()
    config.read('config.ini')
    username = urllib.parse.quote_plus(config.get('db', 'user'))
    password = urllib.parse.quote_plus(config.get('db', 'password'))
    mongo_client = MongoClient('mongodb://%s:%s@' % (username, password) + config.get('db', 'db_url'))
    db_connection = mongo_client[config.get('db', 'db_name')]
    return mongo_client, db_connection[collection]


def get_match_statistics(collection_name):
    db_connection, collection = create_connection(collection_name)
    teams = collection.distinct('team')
    team1, team2 = {}, {}
    team1['name'], team2['name'] = teams[:2]
    for t in [team1, team2]:
        t['positive_tweets'] = collection.count({'team': t['name'], 'polarity': {'$gt': 0}})
        t['negative_tweets'] = collection.count({'team': t['name'], 'polarity': {'$lt': 0}})
    print(collection.count())
    print(team1)
    print(team2)
    db_connection.close()


if __name__ == '__main__':
    while True:
        coll = input('Collection name: ')
        get_match_statistics(coll)
