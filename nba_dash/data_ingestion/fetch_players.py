import logging
from nba_api.stats.static import players
from nba_api.stats.endpoints import CommonPlayerInfo
import mysql.connector
import pymysql
from database.config import get_db_connection


logging.basicConfig(level=logging.INFO)

def fetch_players():
    """Get all active NBA players info."""

    active_player_ids = [player['id'] for player in players.get_active_players()]
    player_info_list = []
    
    for player_id in active_player_ids:
        try:
            player_data = CommonPlayerInfo(player_id=player_id).get_normalized_dict()
            common_info = player_data['CommonPlayerInfo'][0]

            player_info_list.append((
                common_info['PERSON_ID'],
                common_info['DISPLAY_FIRST_LAST'],
                common_info['POSITION'],
                common_info['TEAM_ID'], 
                common_info['TEAM_NAME'] 
            ))
        except Exception as e:
            print(f"Error fetching player {player_id}: {e}")

    # Batch insert into the database
    if player_info_list:
        insert_players_batch(player_info_list)



def insert_players_batch(player_info_list):
    """Insert a batch of player records into the database."""
    
    connection = get_db_connection()
    cursor = connection.cursor()

    sql = """
    INSERT INTO players (player_id, full_name, position, team_id, team_name)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        full_name = VALUES(full_name),
        position = VALUES(position),
        team_id = VALUES(team_id),
        team_name = VALUES(team_name)
    """

    cursor.executemany(sql, player_info_list)  # Efficient batch insert
    connection.commit()
    connection.close()
    print(f"Inserted {len(player_info_list)} players successfully.")

if __name__ == "__main__":
    logging.info("Fetching players...")
    fetch_players()