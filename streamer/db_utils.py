from pymongo import MongoClient
from configparser import ConfigParser
import urllib.parse
from datetime import datetime
from datetime import timedelta


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
    team1, team2, none = {}, {}, {}
    team1['name'], team2['name'], none['name'] = teams[:3]
    for t in [team1, team2, none]:
        t['positive_tweets'] = collection.count({'team': t['name'], 'polarity': {'$gt': 0}})
        t['negative_tweets'] = collection.count({'team': t['name'], 'polarity': {'$lt': 0}})
        t['neutral_tweets'] = collection.count({'team': t['name'], 'polarity': 0})
        t['discarded_tweets'] = collection.count({'team': 'None'})

    print("Total number of tweets : {}\n".format(collection.count()))

    for x in [team1, team2, none]:
        if x['name'] != "None":
            print('{} : {}'.format(x['name'], x['positive_tweets'] / (x['positive_tweets'] + x['negative_tweets'])))
    db_connection.close()


def get_match_statistics_time(collection_name, time):
    db_connection, collection = create_connection(collection_name)
    teams = collection.distinct('team')
    team1, team2, none = {}, {}, {}
    team1['name'], team2['name'], none['name'] = teams[:3]
    for minutes in [25, 60, 85, 110]:
        datetime_object = datetime.strptime(time, '%Y-%m-%d %H:%M')
        query_time = datetime_object + timedelta(minutes=minutes)
        match_time = query_time.strftime('%Y-%m-%d %H:%M')
        for t in [team1, team2, none]:
            t['positive_tweets'] = collection.count({'team': t['name'], 'polarity': {'$gt': 0}, 'create_date': {'$lt': match_time}})
            t['negative_tweets'] = collection.count({'team': t['name'], 'polarity': {'$lt': 0}, 'create_date': {'$lt': match_time}})
            t['neutral_tweets'] = collection.count({'team': t['name'], 'polarity': 0, 'create_date': {'$lt': match_time}})
            t['discarded_tweets'] = collection.count({'team': 'None', 'create_date': {'$lt': match_time}})
        for x in [team1, team2, none]:
            if x['name'] != "None":
                print('{} : {}'.format(x['name'], x['positive_tweets'] / (x['positive_tweets'] + x['negative_tweets'])))
    db_connection.close()

    print("Total number of tweets : {}\n".format(collection.count()))



if __name__ == '__main__':
    while True:
        time_question = input("By my time? Y/N: ")
        coll = input('Collection name: ')
        if time_question.lower() == 'y':
            time = input('Before what time? YYYY-MM-DD HH:MM: ')

            get_match_statistics_time(coll, time)
        else:
            get_match_statistics(coll)
