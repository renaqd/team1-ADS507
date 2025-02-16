import logging
import time
import random
from nba_api.stats.static import players
from nba_api.stats.endpoints import CommonPlayerInfo, CommonAllPlayers
from requests.exceptions import Timeout, ConnectionError
from database.config import get_db_connection

logging.basicConfig(level=logging.INFO)

def fetch_player_with_retry(player_id, max_retries=3, base_delay=0): # consider changing base_delay to 1
    """
    Fetch player data with retry logic and exponential backoff
    """
    for attempt in range(max_retries):
        try:
            # Add a random delay before each request to avoid rate limiting
            time.sleep(base_delay + random.uniform(0, 1))
            
            player_data = CommonPlayerInfo(
                player_id=player_id,
                timeout=60  # Increase timeout to 60 seconds
            ).get_normalized_dict()
            
            return player_data['CommonPlayerInfo'][0]
            
        except (Timeout, ConnectionError) as e:
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            logging.warning(f"Attempt {attempt + 1}/{max_retries} failed for player {player_id}. "
                          f"Retrying in {delay:.2f} seconds... Error: {e}")
            time.sleep(delay)
    
    raise Exception(f"Failed to fetch player {player_id} after {max_retries} attempts")

def fetch_players():
    """Get all active NBA players info."""
    # active_player_ids = [player['id'] for player in players.get_active_players()]
    # removed the above line and added the following two lines due to roster changes
    all_players = CommonAllPlayers(is_only_current_season=0, league_id="00", season="2024-25").get_normalized_dict()['CommonAllPlayers']
    all_players_year = [player for player in all_players if player['TO_YEAR'] == '2024']
    active_player_ids = [player['PERSON_ID'] for player in all_players_year]

    player_info_list = []
    
    # Process players in smaller batches
    batch_size = 5
    for i in range(0, len(active_player_ids), batch_size):
        batch = active_player_ids[i:i + batch_size]
        
        for player_id in batch:
            try:
                common_info = fetch_player_with_retry(player_id)
                player_info_list.append((
                    common_info['PERSON_ID'],
                    common_info['DISPLAY_FIRST_LAST'],
                    common_info['POSITION'],
                    common_info['TEAM_ID'], 
                ))
                logging.info(f"Successfully fetched data for {common_info['DISPLAY_FIRST_LAST']}")
                
            except Exception as e:
                logging.error(f"Error fetching player {player_id}: {e}")
                continue
        
        # Add a delay between batches
        time.sleep(5)
    
    if player_info_list:
        insert_players_batch(player_info_list)
    else:
        logging.warning("No player data was collected to insert into database")

def insert_players_batch(player_info_list):
    """Insert a batch of player records into the database."""
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = """
        INSERT INTO players (player_id, full_name, position, team_id)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            full_name = VALUES(full_name),
            position = VALUES(position),
            team_id = VALUES(team_id)
        """
        cursor.executemany(sql, player_info_list)
        connection.commit()
        logging.info(f"Inserted {len(player_info_list)} players successfully.")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    logging.info("Fetching players...")
    fetch_players()