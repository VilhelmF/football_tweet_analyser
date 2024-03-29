from pymongo import MongoClient
from configparser import ConfigParser
from streamer.db_utils import create_connection
import urllib.parse


class Team:
    def __init__(self, team, *args):
        self.name = team
        self.players = None
        self.game_hashtag = None
        self.hashtags = None
        self.names = None
        self.db_init()

    def db_init(self):
        config = ConfigParser()
        config.read('config.ini')
        mongo_client, teams = create_connection(config.get('db', 'team_collection'))
        team = teams.find_one({'team': self.name})
        self.players = team['players']
        self.game_hashtag = team['game_hashtag']
        self.hashtags = team['hashtags']
        self.names = team['names']
        mongo_client.close()

    def is_player(self, words):
        for player in self.players:
            for word in words:
                if word.lower() in player.lower():
                    return True


if __name__ == '__main__':
    Liverpool = Team('Liverpool')
    print(Liverpool.is_player(['haha', 'fuck', 'coutinho', 'can']))
