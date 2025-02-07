import pandas as pd
from nba_api.stats.endpoints import hustlestatsboxscore
from config import connect_to_mysql, execute_query
from mysql.connector import Error
import logging
from time import sleep

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_all_game_ids(connection):
    """
    Fetch all unique game IDs from the nba_team_game_logs table
    """
    try:
        query = """
        SELECT DISTINCT GAME_ID 
        FROM nba_team_game_logs 
        WHERE GAME_ID NOT IN (
            SELECT GAME_ID 
            FROM hustle_stats_available
        )
        ORDER BY GAME_ID
        """
        
        results = execute_query(connection, query, fetch=True)
        if results:
            game_ids = [row[0] for row in results]
            logger.info(f"Found {len(game_ids)} games to process")
            return game_ids
        return []
        
    except Error as e:
        logger.error(f"Error fetching game IDs: {e}")
        return []

def fetch_hustle_stats(game_id):
    """
    Fetch hustle stats for a specific game from the NBA API
    """
    try:
        hustle_stats = hustlestatsboxscore.HustleStatsBoxScore(game_id=game_id)
        sleep(1)  # Rate limiting
        
        hustle_stats_available_df = pd.DataFrame(hustle_stats.hustle_stats_available.get_dict()['data'],
                                               columns=hustle_stats.hustle_stats_available.get_dict()['headers'])
        
        player_stats_df = pd.DataFrame(hustle_stats.player_stats.get_dict()['data'],
                                     columns=hustle_stats.player_stats.get_dict()['headers'])
        
        logger.info(f"Successfully fetched hustle stats for game {game_id}")
        return hustle_stats_available_df, player_stats_df
        
    except Exception as e:
        logger.error(f"Error fetching hustle stats for game {game_id}: {e}")
        return None, None

def insert_hustle_stats_available(connection, df):
    """Insert or update hustle stats availability data"""
    try:
        insert_query = """
        INSERT INTO hustle_stats_available (GAME_ID, HUSTLE_STATUS)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            HUSTLE_STATUS = VALUES(HUSTLE_STATUS),
            updated_at = CURRENT_TIMESTAMP
        """
        
        values = df[['GAME_ID', 'HUSTLE_STATUS']].values.tolist()
        
        for value in values:
            execute_query(connection, insert_query, value)
            
        logger.info(f"Successfully inserted/updated hustle stats availability for {len(values)} games")
        
    except Error as e:
        logger.error(f"Error inserting hustle stats availability data: {e}")

def insert_player_hustle_stats(connection, df):
    """Insert or update player hustle stats data"""
    try:
        insert_query = """
        INSERT INTO hustle_player_stats (
            GAME_ID, TEAM_ID, TEAM_ABBREVIATION, TEAM_CITY, PLAYER_ID, 
            PLAYER_NAME, START_POSITION, COMMENT, MINUTES, PTS,
            CONTESTED_SHOTS, CONTESTED_SHOTS_2PT, CONTESTED_SHOTS_3PT,
            DEFLECTIONS, CHARGES_DRAWN, SCREEN_ASSISTS, SCREEN_AST_PTS,
            OFF_LOOSE_BALLS_RECOVERED, DEF_LOOSE_BALLS_RECOVERED,
            LOOSE_BALLS_RECOVERED, OFF_BOXOUTS, DEF_BOXOUTS,
            BOX_OUT_PLAYER_TEAM_REBS, BOX_OUT_PLAYER_REBS, BOX_OUTS
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            TEAM_ABBREVIATION = VALUES(TEAM_ABBREVIATION),
            TEAM_CITY = VALUES(TEAM_CITY),
            PLAYER_NAME = VALUES(PLAYER_NAME),
            START_POSITION = VALUES(START_POSITION),
            COMMENT = VALUES(COMMENT),
            MINUTES = VALUES(MINUTES),
            PTS = VALUES(PTS),
            CONTESTED_SHOTS = VALUES(CONTESTED_SHOTS),
            CONTESTED_SHOTS_2PT = VALUES(CONTESTED_SHOTS_2PT),
            CONTESTED_SHOTS_3PT = VALUES(CONTESTED_SHOTS_3PT),
            DEFLECTIONS = VALUES(DEFLECTIONS),
            CHARGES_DRAWN = VALUES(CHARGES_DRAWN),
            SCREEN_ASSISTS = VALUES(SCREEN_ASSISTS),
            SCREEN_AST_PTS = VALUES(SCREEN_AST_PTS),
            OFF_LOOSE_BALLS_RECOVERED = VALUES(OFF_LOOSE_BALLS_RECOVERED),
            DEF_LOOSE_BALLS_RECOVERED = VALUES(DEF_LOOSE_BALLS_RECOVERED),
            LOOSE_BALLS_RECOVERED = VALUES(LOOSE_BALLS_RECOVERED),
            OFF_BOXOUTS = VALUES(OFF_BOXOUTS),
            DEF_BOXOUTS = VALUES(DEF_BOXOUTS),
            BOX_OUT_PLAYER_TEAM_REBS = VALUES(BOX_OUT_PLAYER_TEAM_REBS),
            BOX_OUT_PLAYER_REBS = VALUES(BOX_OUT_PLAYER_REBS),
            BOX_OUTS = VALUES(BOX_OUTS),
            updated_at = CURRENT_TIMESTAMP
        """
        
        columns = ['GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_CITY', 'PLAYER_ID',
                  'PLAYER_NAME', 'START_POSITION', 'COMMENT', 'MINUTES', 'PTS',
                  'CONTESTED_SHOTS', 'CONTESTED_SHOTS_2PT', 'CONTESTED_SHOTS_3PT',
                  'DEFLECTIONS', 'CHARGES_DRAWN', 'SCREEN_ASSISTS', 'SCREEN_AST_PTS',
                  'OFF_LOOSE_BALLS_RECOVERED', 'DEF_LOOSE_BALLS_RECOVERED',
                  'LOOSE_BALLS_RECOVERED', 'OFF_BOXOUTS', 'DEF_BOXOUTS',
                  'BOX_OUT_PLAYER_TEAM_REBS', 'BOX_OUT_PLAYER_REBS', 'BOX_OUTS']
        
        values = df[columns].values.tolist()
        
        # Execute insert in batches
        batch_size = 100
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            for row in batch:
                execute_query(connection, insert_query, row)
            logger.info(f"Processed batch {i//batch_size + 1} of {(len(values)-1)//batch_size + 1}")
        
        logger.info(f"Successfully processed {len(df)} player hustle stats")
        
    except Error as e:
        logger.error(f"Error inserting player hustle stats: {e}")

def run_fetch_hustle_stats():
    """
    Main function to fetch and insert hustle stats data for all games
    """
    connection = None
    try:
        connection = connect_to_mysql()
        if not connection:
            return
            
        game_ids = get_all_game_ids(connection)
        
        if not game_ids:
            logger.info("No new games to process")
            return
            
        total_games = len(game_ids)
        for i, game_id in enumerate(game_ids, 1):
            logger.info(f"Processing game {i} of {total_games} ({game_id})")
            
            hustle_stats_df, player_stats_df = fetch_hustle_stats(game_id)
            
            if hustle_stats_df is not None and player_stats_df is not None:
                insert_hustle_stats_available(connection, hustle_stats_df)
                insert_player_hustle_stats(connection, player_stats_df)
            
            # Commit after each game to save progress
            connection.commit()
            
            # Rate limiting
            sleep(1)
            
        logger.info(f"Completed processing {total_games} games")
            
    finally:
        if connection and connection.is_connected():
            connection.close()
            logger.info("MySQL connection closed")

if __name__ == "__main__":
    run_fetch_hustle_stats()