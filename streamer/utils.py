from streamer.db_utils import create_connection


def valid_team(conf, team):

    # Get the teams from the database and extract the given team
    mongo_client, teams = create_connection(conf.get('db', 'team_collection'))
    result = teams.find_one({'team': team})

    # Close the connection
    mongo_client.close()

    # Return True if the team exists
    return result is not None
