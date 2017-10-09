import csv
from pymongo import MongoClient


def parse_teams():
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
    mongo_client = MongoClient('mongodb://127.0.0.1:27017/')
    db_connection = mongo_client['tweet']
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
    # save_in_db(premier_league_teams)