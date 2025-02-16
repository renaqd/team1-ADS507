import logging
import time
from nba_api.stats.static import teams
from nba_api.stats.endpoints import TeamInfoCommon
from database.config import get_db_connection
from requests.exceptions import Timeout, ConnectionError
import random

logging.basicConfig(level=logging.INFO)

def fetch_team_with_retry(team_id, max_retries=3, base_delay=0):
    """
    Fetch team data with retry logic and exponential backoff
    """
    for attempt in range(max_retries):
        try:
            # Add a random delay before each request to avoid rate limiting
            time.sleep(base_delay + random.uniform(0, 1))
            
            team_data = TeamInfoCommon(
                team_id=team_id,
                timeout=60  # Increase timeout to 60 seconds
            ).get_normalized_dict()
            
            return team_data['TeamInfoCommon'][0]
            
        except (Timeout, ConnectionError) as e:
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            logging.warning(f"Attempt {attempt + 1}/{max_retries} failed for team {team_id}. "
                          f"Retrying in {delay:.2f} seconds... Error: {e}")
            time.sleep(delay)
    
    raise Exception(f"Failed to fetch team {team_id} after {max_retries} attempts")

def fetch_teams():
    """Get all active NBA teams info."""
    active_team_ids = [team['id'] for team in teams.get_teams()]
    team_info_list = []
    
    for team_id in active_team_ids:
        try:
            common_info = fetch_team_with_retry(team_id)
            
            # Set default values for stats that might not be available
            wins = common_info.get('W', 0)
            losses = common_info.get('L', 0)
            win_pct = common_info.get('PCT', 0.0)
            
            team_info_list.append((
                common_info['TEAM_ID'],
                common_info.get('SEASON_YEAR', '2023-24'),
                common_info['TEAM_CITY'],
                common_info['TEAM_NAME'],
                common_info['TEAM_ABBREVIATION'],
                common_info['TEAM_CONFERENCE'],
                wins,
                losses,
                win_pct
            ))
            logging.info(f"Successfully fetched data for {common_info['TEAM_CITY']} {common_info['TEAM_NAME']}")
            
        except Exception as e:
            logging.error(f"Error fetching team {team_id}: {e}")
            continue
    
    if team_info_list:
        insert_team_batch(team_info_list)
    else:
        logging.warning("No team data was collected to insert into database")

def insert_team_batch(team_info_list):
    """Insert a batch of team records into the database."""
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = """
        INSERT INTO teams (team_id, season_year, team_city, team_name, team_abbreviation, 
                         team_conference, wins, losses, win_pct)
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
        cursor.executemany(sql, team_info_list)
        connection.commit()
        print(f"Inserted {len(team_info_list)} teams successfully.")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    logging.info("Fetching teams...")
    fetch_teams()