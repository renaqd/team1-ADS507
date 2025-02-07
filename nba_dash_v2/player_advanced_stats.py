import pandas as pd
import numpy as np
from nba_api.stats.endpoints import boxscoreadvancedv3
from config import connect_to_mysql, execute_query
import time
from datetime import datetime
from mysql.connector import Error
# import mysql.connector


def create_advanced_stats_tables(connection):
    cursor = None
    try:
        cursor = connection.cursor(buffered=True)
        
        # Create player_stats table
        create_player_stats_query = """
        CREATE TABLE IF NOT EXISTS player_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            game_id VARCHAR(10),
            team_id INT,
            team_city VARCHAR(50),
            team_name VARCHAR(50),
            team_tricode VARCHAR(3),
            team_slug VARCHAR(50),
            person_id INT,
            first_name VARCHAR(50),
            family_name VARCHAR(50),
            name_i VARCHAR(50),
            player_slug VARCHAR(50),
            position VARCHAR(10),
            comment TEXT,
            jersey_num VARCHAR(10),
            minutes VARCHAR(10),  -- Changed from FLOAT to VARCHAR
            estimated_offensive_rating FLOAT,
            offensive_rating FLOAT,
            estimated_defensive_rating FLOAT,
            defensive_rating FLOAT,
            estimated_net_rating FLOAT,
            net_rating FLOAT,
            assist_percentage FLOAT,
            assist_to_turnover FLOAT,
            assist_ratio FLOAT,
            offensive_rebound_percentage FLOAT,
            defensive_rebound_percentage FLOAT,
            rebound_percentage FLOAT,
            turnover_ratio FLOAT,
            effective_field_goal_percentage FLOAT,
            true_shooting_percentage FLOAT,
            usage_percentage FLOAT,
            estimated_usage_percentage FLOAT,
            estimated_pace FLOAT,
            pace FLOAT,
            pace_per40 FLOAT,
            possessions FLOAT,
            pie FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_player_game (game_id, person_id)
        )
        """
        
        # Create team_stats table with similar change
        create_team_stats_query = """
        CREATE TABLE IF NOT EXISTS team_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            game_id VARCHAR(10),
            team_id INT,
            team_city VARCHAR(50),
            team_name VARCHAR(50),
            team_tricode VARCHAR(3),
            team_slug VARCHAR(50),
            minutes VARCHAR(10),  -- Changed from FLOAT to VARCHAR
            estimated_offensive_rating FLOAT,
            offensive_rating FLOAT,
            estimated_defensive_rating FLOAT,
            defensive_rating FLOAT,
            estimated_net_rating FLOAT,
            net_rating FLOAT,
            assist_percentage FLOAT,
            assist_to_turnover FLOAT,
            assist_ratio FLOAT,
            offensive_rebound_percentage FLOAT,
            defensive_rebound_percentage FLOAT,
            rebound_percentage FLOAT,
            estimated_team_turnover_percentage FLOAT,
            turnover_ratio FLOAT,
            effective_field_goal_percentage FLOAT,
            true_shooting_percentage FLOAT,
            usage_percentage FLOAT,
            estimated_usage_percentage FLOAT,
            estimated_pace FLOAT,
            pace FLOAT,
            pace_per40 FLOAT,
            possessions FLOAT,
            pie FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_team_game (game_id, team_id)
        )
        """
        
        # Drop existing tables first
        cursor.execute("DROP TABLE IF EXISTS player_stats")
        cursor.execute("DROP TABLE IF EXISTS team_stats")
        
        # Create new tables
        cursor.execute(create_player_stats_query)
        cursor.execute(create_team_stats_query)
        connection.commit()
        print("Advanced stats tables created or already exist")
        
    except Error as e:
        print(f"Error creating tables: {e}")
    finally:
        if cursor:
            cursor.close()

def preprocess_dataframe(df):
    """Preprocess DataFrame to handle special data types"""
    df_copy = df.copy()
    
    # Convert minutes to string format if it exists
    if 'minutes' in df_copy.columns:
        df_copy['minutes'] = df_copy['minutes'].astype(str)
    
    # Handle any NaN values
    df_copy = df_copy.replace({np.nan: None})
    
    return df_copy

def insert_player_stats(connection, df):
    """Insert player stats into database"""
    if df.empty:
        return
        
    cursor = None
    try:
        cursor = connection.cursor(buffered=True)
        
        # Create a copy and preprocess
        df_copy = preprocess_dataframe(df)
        
        # Define column mapping
        column_mapping = {
            'gameId': 'game_id',
            'teamId': 'team_id',
            # ... (rest of the mapping stays the same)
        }
        
        # Rename columns
        df_copy = df_copy.rename(columns=column_mapping)
        
        # Get column names and create placeholders
        columns = list(column_mapping.values())
        placeholders = ", ".join(["%s"] * len(columns))
        
        # Create INSERT query
        insert_query = f"""
        INSERT INTO player_stats 
        ({', '.join(columns)}) 
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        {', '.join(f"{col}=VALUES({col})" for col in columns if col not in ['game_id', 'person_id'])}
        """
        
        # Insert data
        values = df_copy[columns].values.tolist()
        cursor.executemany(insert_query, values)
        connection.commit()
        print(f"Successfully inserted {len(df)} player records")
        
    except Error as e:
        print(f"Error inserting player stats: {e}")
        print("Error details:", e.args)
        print("Available columns in DataFrame:", df.columns.tolist())
        cursor.execute("DESCRIBE player_stats")
        print("Table structure:", cursor.fetchall())
    finally:
        if cursor:
            cursor.close()

def get_game_ids(connection):
    """Get game IDs from nba_team_game_logs table"""
    cursor = None
    try:
        cursor = connection.cursor(buffered=True)
        cursor.execute("SELECT DISTINCT game_id FROM nba_team_game_logs")
        return [row[0] for row in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()

def fetch_advanced_stats(game_id):
    """Fetch advanced stats for a specific game"""
    try:
        # Add delay to avoid hitting API rate limits
        time.sleep(1)
        
        # Fetch box score data
        box_score = boxscoreadvancedv3.BoxScoreAdvancedV3(
            game_id=game_id,
            start_period=1,
            end_period=1,
            start_range=0,
            end_range=0,
            range_type=0
        )
        
        # Get both player and team stats
        player_stats = box_score.player_stats.get_data_frame()
        team_stats = box_score.team_stats.get_data_frame()
        
        return player_stats, team_stats
        
    except Exception as e:
        print(f"Error fetching advanced stats for game {game_id}: {e}")
        return None, None

def insert_player_stats(connection, df):
    """Insert player stats into database"""
    if df.empty:
        return
        
    cursor = None
    try:
        cursor = connection.cursor(buffered=True)
        
        # Create a copy of the DataFrame
        df_copy = df.copy()
        
        # Define column mapping
        column_mapping = {
            'gameId': 'game_id',
            'teamId': 'team_id',
            'teamCity': 'team_city',
            'teamName': 'team_name',
            'teamTricode': 'team_tricode',
            'teamSlug': 'team_slug',
            'personId': 'person_id',
            'firstName': 'first_name',
            'familyName': 'family_name',
            'nameI': 'name_i',
            'playerSlug': 'player_slug',
            'position': 'position',
            'comment': 'comment',
            'jerseyNum': 'jersey_num',
            'minutes': 'minutes',
            'estimatedOffensiveRating': 'estimated_offensive_rating',
            'offensiveRating': 'offensive_rating',
            'estimatedDefensiveRating': 'estimated_defensive_rating',
            'defensiveRating': 'defensive_rating',
            'estimatedNetRating': 'estimated_net_rating',
            'netRating': 'net_rating',
            'assistPercentage': 'assist_percentage',
            'assistToTurnover': 'assist_to_turnover',
            'assistRatio': 'assist_ratio',
            'offensiveReboundPercentage': 'offensive_rebound_percentage',
            'defensiveReboundPercentage': 'defensive_rebound_percentage',
            'reboundPercentage': 'rebound_percentage',
            'turnoverRatio': 'turnover_ratio',
            'effectiveFieldGoalPercentage': 'effective_field_goal_percentage',
            'trueShootingPercentage': 'true_shooting_percentage',
            'usagePercentage': 'usage_percentage',
            'estimatedUsagePercentage': 'estimated_usage_percentage',
            'estimatedPace': 'estimated_pace',
            'pace': 'pace',
            'pacePer40': 'pace_per40',
            'possessions': 'possessions',
            'PIE': 'pie'
        }
        
        # Rename columns
        df_copy = df_copy.rename(columns=column_mapping)
        
        # Get column names and create placeholders
        columns = list(column_mapping.values())
        placeholders = ", ".join(["%s"] * len(columns))
        
        # Create INSERT query
        insert_query = f"""
        INSERT INTO player_stats 
        ({', '.join(columns)}) 
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        {', '.join(f"{col}=VALUES({col})" for col in columns if col not in ['game_id', 'person_id'])}
        """
        
        # Insert data
        values = df_copy[columns].values.tolist()
        cursor.executemany(insert_query, values)
        connection.commit()
        print(f"Successfully inserted {len(df)} player records")
        
    except Error as e:
        print(f"Error inserting player stats: {e}")
        print("Error details:", e.args)
        print("Available columns in DataFrame:", df.columns.tolist())
        cursor.execute("DESCRIBE player_stats")
        print("Table structure:", cursor.fetchall())
    finally:
        if cursor:
            cursor.close()

def insert_team_stats(connection, df):
    """Insert team stats into database"""
    if df.empty:
        return
        
    cursor = None
    try:
        cursor = connection.cursor(buffered=True)
        
        # Create a copy of the DataFrame
        df_copy = df.copy()
        
        # Define column mapping
        column_mapping = {
            'gameId': 'game_id',
            'teamId': 'team_id',
            'teamCity': 'team_city',
            'teamName': 'team_name',
            'teamTricode': 'team_tricode',
            'teamSlug': 'team_slug',
            'minutes': 'minutes',
            'estimatedOffensiveRating': 'estimated_offensive_rating',
            'offensiveRating': 'offensive_rating',
            'estimatedDefensiveRating': 'estimated_defensive_rating',
            'defensiveRating': 'defensive_rating',
            'estimatedNetRating': 'estimated_net_rating',
            'netRating': 'net_rating',
            'assistPercentage': 'assist_percentage',
            'assistToTurnover': 'assist_to_turnover',
            'assistRatio': 'assist_ratio',
            'offensiveReboundPercentage': 'offensive_rebound_percentage',
            'defensiveReboundPercentage': 'defensive_rebound_percentage',
            'reboundPercentage': 'rebound_percentage',
            'estimatedTeamTurnoverPercentage': 'estimated_team_turnover_percentage',
            'turnoverRatio': 'turnover_ratio',
            'effectiveFieldGoalPercentage': 'effective_field_goal_percentage',
            'trueShootingPercentage': 'true_shooting_percentage',
            'usagePercentage': 'usage_percentage',
            'estimatedUsagePercentage': 'estimated_usage_percentage',
            'estimatedPace': 'estimated_pace',
            'pace': 'pace',
            'pacePer40': 'pace_per40',
            'possessions': 'possessions',
            'PIE': 'pie'
        }
        
        # Rename columns
        df_copy = df_copy.rename(columns=column_mapping)
        
        # Get column names and create placeholders
        columns = list(column_mapping.values())
        placeholders = ", ".join(["%s"] * len(columns))
        
        # Create INSERT query
        insert_query = f"""
        INSERT INTO team_stats 
        ({', '.join(columns)}) 
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        {', '.join(f"{col}=VALUES({col})" for col in columns if col not in ['game_id', 'team_id'])}
        """
        
        # Insert data
        values = df_copy[columns].values.tolist()
        cursor.executemany(insert_query, values)
        connection.commit()
        print(f"Successfully inserted {len(df)} team records")
        
    except Error as e:
        print(f"Error inserting team stats: {e}")
        print("Error details:", e.args)
        print("Available columns in DataFrame:", df.columns.tolist())
        cursor.execute("DESCRIBE team_stats")
        print("Table structure:", cursor.fetchall())
    finally:
        if cursor:
            cursor.close()

def print_stats_summary(connection):
    """Print summary of advanced stats data"""
    cursor = None
    try:
        cursor = connection.cursor(buffered=True)
        
        # Get counts
        cursor.execute("SELECT COUNT(DISTINCT game_id) FROM player_stats")
        player_games = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT game_id) FROM team_stats")
        team_games = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT person_id) FROM player_stats")
        total_players = cursor.fetchone()[0]
        
        # Print summary
        print("\nAdvanced Stats Summary:")
        print(f"Games with player stats: {player_games}")
        print(f"Games with team stats: {team_games}")
        print(f"Total unique players: {total_players}")
        
    except Error as e:
        print(f"Error getting stats summary: {e}")
    finally:
        if cursor:
            cursor.close()


def run_fetch_advanced_stats():
    """Main function to fetch and insert advanced stats data"""

    connection = None
    try:
        # Connect to MySQL
        connection = connect_to_mysql()
        if not connection:
            return
        
        # Create tables
        create_advanced_stats_tables(connection)
        
        # Get game IDs
        game_ids = get_game_ids(connection)
        print(f"Found {len(game_ids)} games to process")
        
        # Process each game
        for i, game_id in enumerate(game_ids, 1):
            print(f"\nProcessing game {i} of {len(game_ids)} (ID: {game_id})")
            player_stats, team_stats = fetch_advanced_stats(game_id)
            
            if player_stats is not None:
                insert_player_stats(connection, player_stats)
            if team_stats is not None:
                insert_team_stats(connection, team_stats)
        
        # Print summary
        print_stats_summary(connection)
            
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("\nMySQL connection closed")




            

if __name__ == "__main__":
    run_fetch_advanced_stats()