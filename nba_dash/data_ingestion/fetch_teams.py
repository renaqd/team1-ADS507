import logging
from nba_api.stats.static import teams
from nba_api.stats.endpoints import TeamInfoCommon
import mysql.connector
import pymysql
from database.config import get_db_connection


logging.basicConfig(level=logging.INFO)

def fetch_teams():
    """Get all active NBA players info."""

    active_team_ids = [team['id'] for team in teams.get_teams()]
    team_info_list = []
    
    for team_id in active_team_ids:
        try:
            team_data = TeamInfoCommon(team_id=team_id).get_normalized_dict()
            common_info = team_data['TeamInfoCommon'][0]

            team_info_list.append((
                common_info['TEAM_ID'],
                common_info['SEASON_YEAR'],
                common_info['TEAM_CITY'],
                common_info['TEAM_NAME'],
                common_info['TEAM_ABBREVIATION'],
                common_info['TEAM_CONFERENCE'],
                common_info['WINS'],
                common_info['LOSSES'],
                common_info['WIN_PCT']
            ))
        except Exception as e:
            print(f"Error fetching player {team_id}: {e}")

    # Batch insert into the database
    if team_info_list:
        insert_team_batch(team_info_list)


def insert_team_batch(team_info_list):
    """Insert a batch of player records into the database."""
    
    connection = get_db_connection()
    cursor = connection.cursor()

    sql = """
    INSERT INTO teams (team_id, season_year, team_city, team_name, team_abbreviation, team_conference, wins, losses, win_pct)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        season_year = VALUES(season_year),
        team_city = VALUES(team_city),
        team_name = VALUES(team_name),
        team_abbreviation = VALUES(team_abbreviation),
        team_conference = VALUES(team_conference),
        wins = VALUES(wins),
        losses = VALUES(losses),
        win_pct = VALUES(win_pct)
    """

    cursor.executemany(sql, team_info_list)  # Efficient batch insert
    connection.commit()
    connection.close()
    print(f"Inserted {len(team_info_list)} players successfully.")

if __name__ == "__main__":
    logging.info("Fetching teams...")
    fetch_teams()