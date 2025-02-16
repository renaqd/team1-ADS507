import logging
import time
import random
from datetime import datetime, timedelta
from nba_api.stats.endpoints import HustleStatsBoxScore, LeagueGameFinder
from requests.exceptions import Timeout, ConnectionError
from database.config import get_db_connection

logging.basicConfig(level=logging.INFO)

def fetch_game_with_retry(game_id, max_retries=3, base_delay=2):
    """
    Fetch hustle stats for a game with retry logic and exponential backoff
    """
    for attempt in range(max_retries):
        try:
            # Add a random delay before each request
            time.sleep(base_delay + random.uniform(0, 1))
            
            game_data = HustleStatsBoxScore(
                game_id=str(game_id),  # Ensure game_id is string
                timeout=60
            ).get_normalized_dict()
            
            # Check if PlayerStats exists and is a list
            if 'PlayerStats' not in game_data or not isinstance(game_data['PlayerStats'], list):
                raise ValueError(f"Invalid data format for game {game_id}")
                
            return game_data['PlayerStats']
            
        except (Timeout, ConnectionError) as e:
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            logging.warning(f"Attempt {attempt + 1}/{max_retries} failed for game {game_id}. "
                          f"Retrying in {delay:.2f} seconds... Error: {e}")
            time.sleep(delay)
    
    raise Exception(f"Failed to fetch game {game_id} after {max_retries} attempts")

# Remove the get game date
def get_game_date(game_id):
    """Extract date from game ID with proper formatting"""
    try:
        # Convert game_id to string if it's an integer
        game_id_str = str(game_id)
        # Extract the date portion (first 8 characters)
        date_str = game_id_str[-8:] if len(game_id_str) > 8 else game_id_str
        return datetime.strptime(date_str, '%Y%m%d').date()
    except ValueError as e:
        logging.error(f"Error parsing date from game_id {game_id}: {e}")
        return None

def fetch_game_ids(days_back=7):
    """
    Fetch game IDs for the last N days
    """
    try:
        nba_team_ids = set(range(1610612737, 1610612767))  # Specify NBA team IDs only

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        all_games = LeagueGameFinder(
            date_from_nullable=start_date.strftime('%m/%d/%Y'),
            date_to_nullable=end_date.strftime('%m/%d/%Y'),
            timeout=60
        ).get_normalized_dict()['LeagueGameFinderResults']

        games = [game for game in all_games if game.get('TEAM_ID') in nba_team_ids]
        
        #if 'LeagueGameFinderResults' not in games:
        #    logging.error("No game results found in API response")
        #    return []
        
        game_data = [
            (str(game['GAME_ID']), datetime.strptime(game['GAME_DATE'], '%Y-%m-%d').date(), game['MATCHUP'])  # Get game_id and date
            for game in games
        ]
        
        logging.info(f"Fetched {len(game_data)} game IDs successfully.")

        return game_data
    
    except Exception as e:
        logging.error(f"Error fetching game IDs: {e}")
        return []

def insert_hustle_stats_batch(stats_list):
    """Insert a batch of hustle stats records into the database."""
    if not stats_list:
        logging.warning("No stats to insert")
        return
        
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = """
        INSERT INTO hustle_stats (
            game_id, player_id, team_id, game_date, matchup, minutes, pts,
            contested_shots, contested_shots_2pt, contested_shots_3pt,
            deflections, charges_drawn, screen_assists, screen_ast_pts,
            off_loose_balls_recovered, def_loose_balls_recovered,
            loose_balls_recovered, off_boxouts, def_boxouts, boxouts
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE 
            minutes = VALUES(minutes),
            pts = VALUES(pts),
            contested_shots = VALUES(contested_shots),
            contested_shots_2pt = VALUES(contested_shots_2pt),
            contested_shots_3pt = VALUES(contested_shots_3pt),
            deflections = VALUES(deflections),
            charges_drawn = VALUES(charges_drawn),
            screen_assists = VALUES(screen_assists),
            screen_ast_pts = VALUES(screen_ast_pts),
            off_loose_balls_recovered = VALUES(off_loose_balls_recovered),
            def_loose_balls_recovered = VALUES(def_loose_balls_recovered),
            loose_balls_recovered = VALUES(loose_balls_recovered),
            off_boxouts = VALUES(off_boxouts),
            def_boxouts = VALUES(def_boxouts),
            boxouts = VALUES(boxouts)
        """
        cursor.executemany(sql, stats_list)
        connection.commit()
        logging.info(f"Inserted {len(stats_list)} hustle stats records successfully.")
    except Exception as e:
        logging.error(f"Error inserting hustle stats: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()

def fetch_hustle_stats(days_back=7):
    """Fetch and store hustle stats for games in the last N days."""
    game_data_list = fetch_game_ids(days_back)
    if not game_data_list:
        logging.warning("No games found for the specified period")
        return
    
    logging.info(f"Found {len(game_data_list)} games to process")
    stats_list = []
    
    # Process games in batches
    batch_size = 5
    max_games = 2 ### adding limit for testing ###

    for i in range(0, min(max_games, len(game_data_list)), batch_size):
        batch = game_data_list[i:i + batch_size]
        
        for game_id, game_date, matchup in batch:
            try:
                player_stats = fetch_game_with_retry(game_id)
            
                for player in player_stats:
                    # Convert minutes from MM:SS format to integer seconds
                    minutes_str = player.get('MINUTES', '0:00')
                    if ':' in str(minutes_str):
                        min_parts = str(minutes_str).split(':')
                        minutes = int(min_parts[0]) * 60 + int(min_parts[1])
                    else:
                        minutes = 0
                    
                    stats_list.append((
                        int(game_id),
                        int(player['PLAYER_ID']),
                        int(player['TEAM_ID']),
                        game_date,
                        matchup,
                        minutes,
                        int(player.get('PTS', 0)),
                        int(player.get('CONTESTED_SHOTS', 0)),
                        int(player.get('CONTESTED_SHOTS_2PT', 0)),
                        int(player.get('CONTESTED_SHOTS_3PT', 0)),
                        int(player.get('DEFLECTIONS', 0)),
                        int(player.get('CHARGES_DRAWN', 0)),
                        int(player.get('SCREEN_ASSISTS', 0)),
                        int(player.get('SCREEN_AST_PTS', 0)),
                        int(player.get('OFF_LOOSE_BALLS_RECOVERED', 0)),
                        int(player.get('DEF_LOOSE_BALLS_RECOVERED', 0)),
                        int(player.get('LOOSE_BALLS_RECOVERED', 0)),
                        int(player.get('OFF_BOXOUTS', 0)),
                        int(player.get('DEF_BOXOUTS', 0)),
                        int(player.get('BOX_OUTS', 0))
                    ))
                
                logging.info(f"Successfully processed game {game_id}")
                
            except Exception as e:
                logging.error(f"Error processing game {game_id}: {e}")
                continue
        
        # Insert batch of stats
        if stats_list:
            insert_hustle_stats_batch(stats_list)
            stats_list = []  # Clear the list after insertion
        
        # Add delay between batches
        time.sleep(5)

if __name__ == "__main__":
    logging.info("Starting hustle stats collection...")
    fetch_hustle_stats()
    logging.info("Hustle stats collection completed!")