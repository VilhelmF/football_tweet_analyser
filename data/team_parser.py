import csv
from pymongo import MongoClient
import urllib.parse
from configparser import ConfigParser


def get_hashtags():
    hashtags = {'Bournemouth': ('BOU', ['AFCB']),
                'Arsenal': ('ARS', ['AFC']),
                'Brighton & Hove Albion': ('BRI', ['BHAFC']),
                'Burnley': ('BUR', ['Clarets']),
                'Chelsea': ('CHE', ['CFC']),
                'Crystal Palace': ('CRY', ['CPFC']),
                'Everton': ('EVE', ['EFC']),
                'Huddersfield Town': ('HUD', ['HTAFC']),
                'Leicester City': ('LEI', ['LCFC']),
                'Liverpool': ('LIV', ['LFC', 'LiverpoolFC']),
                'Manchester City': ('MCI', ['MCFC']),
                'Manchester United': ('MUN', ['MUFC']),
                'Newcastle United': ('NEW', ['NUFC']),
                'Southampton': ('SOU', ['SaintsFC']),
                'Stoke City': ('STK', ['SCFC']),
                'Swansea City': ('SWA', ['Swans']),
                'Tottenham Hotspur': ('TOT', ['COYS', 'THFC', 'Spurs']),
                'Watford': ('WAT', ['WatfordFC']),
                'West Bromwich Albion': ('WBA', ['WBA', 'baggies', 'WBAFC']),
                'West Ham United': ('WHU', ['WHUFC'])
                }
    return hashtags


def parse_teams():
    hashtags = get_hashtags()
    with open('teams.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        premier_leage_teams = []
        working_dict = {}
        new_team = True
        for row in reader:
            if ''.join(row) == '':
                premier_leage_teams.append(working_dict)
                new_team = True
                continue
            if new_team:
                team = ''.join(row)
                working_dict = {'team': team, 'players': []}
                new_team = False
                continue
            else:
                player = ''.join(row)
                player = player.replace('*', '')
                player = player.split(', ')[1] + ' ' + player.split(',')[0]
                working_dict['players'].append(player)
        return premier_leage_teams


def save_in_db(teams):
    config = ConfigParser()
    config.read('../streamer/config.ini')
    username = urllib.parse.quote_plus(config.get('db', 'user'))
    password = urllib.parse.quote_plus(config.get('db', 'password'))
    mongo_client = MongoClient('mongodb://%s:%s@' % (username, password) + config.get('db', 'db_url'))
    db_connection = mongo_client[config.get('db', 'db_name')]
    if 'teams' in db_connection.collection_names():
        print('Collection already exists. Aborting.')
        return
    db_collection = db_connection['teams']
    print('Database connection to established')
    for premier_team in teams:
        object_id = db_collection.insert_one(premier_team).inserted_id
        print('{} : {}'.format(object_id, premier_team['team']))
    mongo_client.close()


if __name__ == '__main__':
    premier_league_teams = parse_teams()
    save_in_db(premier_league_teams)
