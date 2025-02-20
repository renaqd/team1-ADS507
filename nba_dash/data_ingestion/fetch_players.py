import logging
from nba_api.stats.endpoints import CommonAllPlayers
import time
from database.config import get_db_connection

logging.basicConfig(level=logging.INFO)

def insert_players_batch(player_info_list):
    if not player_info_list:
        logging.warning("No players to insert")
        return

    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = """
        INSERT INTO players (player_id, player_name, position, team_id)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            player_name = VALUES(player_name),
            position = VALUES(position),
            team_id = VALUES(team_id)
        """
        cursor.executemany(sql, player_info_list)
        connection.commit()
        logging.info(f"Inserted or updated {len(player_info_list)} players successfully.")
    except Exception as e:
        logging.error(f"Error inserting players: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()

def fetch_players():
    max_retries = 3
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            all_players = CommonAllPlayers(is_only_current_season=1, league_id="00", season="2024-25").get_normalized_dict()['CommonAllPlayers']
            
            player_info_list = []
            for player in all_players:
                player_info = (
                    player['PERSON_ID'],
                    player['DISPLAY_FIRST_LAST'],
                    player['POSITION'],
                    player['TEAM_ID']
                )
                player_info_list.append(player_info)
                logging.info(f"Successfully fetched data for {player['DISPLAY_FIRST_LAST']}")
            
            if player_info_list:
                insert_players_batch(player_info_list)
                logging.info(f"Successfully inserted {len(player_info_list)} players into the database")
            else:
                logging.warning("No player data was collected to insert into database")
            
            return  # Exit the function if successful
        
        except Exception as e:
            logging.error(f"Error fetching players (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error("Max retries reached. Unable to fetch player data.")

if __name__ == "__main__":
    fetch_players()