import logging
import time
from nba_api.stats.endpoints import LeagueStandings
from database.config import get_db_connection
from requests.exceptions import Timeout, ConnectionError
import random

logging.basicConfig(level=logging.INFO)

def fetch_teams_with_retry(max_retries=3, base_delay=1):
    for attempt in range(max_retries):
        try:
            time.sleep(base_delay + random.uniform(0, 1))
            
            standings = LeagueStandings(
                season="2024-25",
                season_type="Regular Season",
                timeout=60
            ).get_normalized_dict()
            
            return standings['Standings']
            
        except (Timeout, ConnectionError) as e:
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            logging.warning(f"Attempt {attempt + 1}/{max_retries} failed for fetching teams. "
                          f"Retrying in {delay:.2f} seconds... Error: {e}")
            time.sleep(delay)
    
    raise Exception(f"Failed to fetch teams after {max_retries} attempts")

def fetch_teams():
    try:
        teams_data = fetch_teams_with_retry()
        team_info_list = []
        
        for team in teams_data:
            team_info_list.append((
                team['TeamID'],
                "2024-25",
                team['TeamCity'],
                team['TeamName'],
                team['TeamAbbreviation'],
                team['Conference'],
                team['WINS'],
                team['LOSSES'],
                team['WinPCT']
            ))
            logging.info(f"Successfully fetched data for {team['TeamCity']} {team['TeamName']}")
        
        if team_info_list:
            insert_team_batch(team_info_list)
        else:
            logging.warning("No team data was collected to insert into database")
    
    except Exception as e:
        logging.error(f"Error fetching teams: {e}")

def insert_team_batch(team_info_list):
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
        print(f"Inserted or updated {len(team_info_list)} teams successfully.")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    logging.info("Fetching recent teams data...")
    fetch_teams()