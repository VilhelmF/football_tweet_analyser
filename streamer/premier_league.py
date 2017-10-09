from pymongo import MongoClient


class PremierLeague:
    def __init__(self):
        self.teams = {}
        self.init_teams()

    def init_teams(self):
        mongo_client = MongoClient('mongodb://127.0.0.1:27017/')
        db_connection = mongo_client['tweet']
        db_collection = db_connection['teams']
        cursor = db_collection.find({})
        for team in cursor:
            self.teams[team['team']] = team['players']
        mongo_client.close()

    def print_teams(self):
        for key, value in self.teams.items():
            print('***' + key + '***')
            for player in value:
                print(player)

    def find_team(self, words, team1, team2):
        print('lol')


if __name__ == '__main__':
    pl = PremierLeague()
    pl.print_teams()
