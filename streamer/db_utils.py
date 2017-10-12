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
        t['positive_tweets'] = collection.count({'sentiment': 'positive'})
        t['negative_tweets'] = collection.count({'sentiment': 'negative'})
    print(team1)
    db_connection.close()


if __name__ == '__main__':
    connection, teams = create_connection('teams')
    print(teams.count())
    connection.close()

    get_match_statistics('teams')
