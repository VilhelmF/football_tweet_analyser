import csv
from pymongo import MongoClient
import urllib.parse
from configparser import ConfigParser


def get_hashtags():
    hashtags = {'Bournemouth': {'hashtags': ('BOU', ['AFCB']), 'names': ['Bournemouth']},
                'Arsenal': {'hashtags': ('ARS', ['AFC']), 'names': ['Arsenal', 'Gunners']},
                'Brighton & Hove Albion': {'hashtags': ('BRI', ['BHAFC']), 'names': ['Brighton']},
                'Burnley': {'hashtags': ('BUR', ['Clarets']), 'names': ['Burnley']},
                'Chelsea': {'hashtags': ('CHE', ['CFC']), 'names': ['Chelsea']},
                'Crystal Palace': {'hashtags': ('CRY', ['CPFC']), 'names': ['Palace']},
                'Everton': {'hashtags': ('EVE', ['EFC']), 'names': ['Everton']},
                'Huddersfield Town': {'hashtags': ('HUD', ['HTAFC']), 'names': ['Huddersfield']},
                'Leicester City': {'hashtags': ('LEI', ['LCFC']), 'names': ['Leicester']},
                'Liverpool': {'hashtags': ('LIV', ['LFC', 'LiverpoolFC']), 'names': ['Liverpool', 'Lpool', 'LFC']},
                'Manchester City': {'hashtags': ('MCI', ['MCFC']), 'names': ['Manchester', 'ManCity', 'City']},
                'Manchester United': {'hashtags': ('MUN', ['MUFC']), 'names': ['United', 'ManUtd', 'Manchester']},
                'Newcastle United': {'hashtags': ('NEW', ['NUFC']), 'names': ['Newcastle']},
                'Southampton': {'hashtags': ('SOU', ['SaintsFC']), 'names': ['Southampton']},
                'Stoke City': {'hashtags': ('STK', ['SCFC']), 'names': ['Stoke']},
                'Swansea City': {'hashtags': ('SWA', ['Swans']), 'names': ['Swansea']},
                'Tottenham Hotspur': {'hashtags': ('TOT', ['COYS', 'THFC', 'Spurs']), 'names': ['Tottenham', 'Spurs']},
                'Watford': {'hashtags': ('WAT', ['WatfordFC']), 'names': ['Watford']},
                'West Bromwich Albion': {'hashtags': ('WBA', ['baggies', 'WBAFC']), 'names': ['West Brom', 'WBA', 'West Bromvich', 'Albion']},
                'West Ham United': {'hashtags': ('WHU', ['WHUFC']), 'names': ['West Ham']}
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
                ht_tuple = hashtags[team]
                working_dict = {'team': team, 'players': [], 'game_hashtag': ht_tuple[0], 'hashtags': ht_tuple[1]}
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
