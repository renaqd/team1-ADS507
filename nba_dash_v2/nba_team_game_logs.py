import time
import pandas as pd
from mysql.connector import Error
from nba_api.stats.endpoints import teamgamelogs
from nba_api.stats.static import teams
from setup_db import connect_to_mysql

def fetch_team_game_logs(team_id, season='2023-24'):
    """Fetch game logs for a specific team and season"""
    try:
        # Add delay to avoid hitting API rate limits
        time.sleep(1)
        
        game_logs = teamgamelogs.TeamGameLogs(
            team_id_nullable=team_id,
            season_nullable=season,
            season_type_nullable='Regular Season'
        )
        
        # Convert to DataFrame
        df = game_logs.get_data_frames()[0]
        
        # Convert GAME_DATE to proper date format
        df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE']).dt.date
        
        return df
        
    except Exception as e:
        print(f"Error fetching game logs for team {team_id}: {e}")
        return None

def insert_game_logs(connection, df):
    """Insert or update game logs data into MySQL table"""
    cursor = None
    try:
        cursor = connection.cursor(buffered=True)
        
        # Create a copy of the DataFrame and rename columns to match table structure
        df_copy = df.copy()
        
        # Define column mapping (from API to database)
        column_mapping = {
            'SEASON_YEAR': 'SEASON_YEAR',
            'TEAM_ID': 'TEAM_ID',
            'TEAM_ABBREVIATION': 'TEAM_ABBREVIATION',
            'TEAM_NAME': 'TEAM_NAME',
            'GAME_ID': 'GAME_ID',
            'GAME_DATE': 'GAME_DATE',
            'MATCHUP': 'MATCHUP',
            'WL': 'WL',
            'MIN': 'MIN',
            'FGM': 'FGM',
            'FGA': 'FGA',
            'FG_PCT': 'FG_PCT',
            'FG3M': 'FG3M',
            'FG3A': 'FG3A',
            'FG3_PCT': 'FG3_PCT',
            'FTM': 'FTM',
            'FTA': 'FTA',
            'FT_PCT': 'FT_PCT',
            'OREB': 'OREB',
            'DREB': 'DREB',
            'REB': 'REB',
            'AST': 'AST',
            'TOV': 'TOV',
            'STL': 'STL',
            'BLK': 'BLK',
            'BLKA': 'BLKA',
            'PF': 'PF',
            'PFD': 'PFD',
            'PTS': 'PTS',
            'PLUS_MINUS': 'PLUS_MINUS',
            'GP_RANK': 'GP_RANK',
            'W_RANK': 'W_RANK',
            'L_RANK': 'L_RANK',
            'W_PCT_RANK': 'W_PCT_RANK',
            'MIN_RANK': 'MIN_RANK',
            'FGM_RANK': 'FGM_RANK',
            'FGA_RANK': 'FGA_RANK',
            'FG_PCT_RANK': 'FG_PCT_RANK',
            'FG3M_RANK': 'FG3M_RANK',
            'FG3A_RANK': 'FG3A_RANK',
            'FG3_PCT_RANK': 'FG3_PCT_RANK',
            'FTM_RANK': 'FTM_RANK',
            'FTA_RANK': 'FTA_RANK',
            'FT_PCT_RANK': 'FT_PCT_RANK',
            'OREB_RANK': 'OREB_RANK',
            'DREB_RANK': 'DREB_RANK',
            'REB_RANK': 'REB_RANK',
            'AST_RANK': 'AST_RANK',
            'TOV_RANK': 'TOV_RANK',
            'STL_RANK': 'STL_RANK',
            'BLK_RANK': 'BLK_RANK',
            'BLKA_RANK': 'BLKA_RANK',
            'PF_RANK': 'PF_RANK',
            'PFD_RANK': 'PFD_RANK',
            'PTS_RANK': 'PTS_RANK',
            'PLUS_MINUS_RANK': 'PLUS_MINUS_RANK'
        }
        
        # Rename columns
        df_copy = df_copy.rename(columns=column_mapping)
        
        # Drop any columns that aren't in our mapping
        df_copy = df_copy[list(column_mapping.values())]
        
        # Get the column names from the mapped DataFrame
        columns = df_copy.columns.tolist()
        
        # Create the placeholders string
        placeholders = ", ".join(["%s"] * len(columns))
        
        # Create the ON DUPLICATE KEY UPDATE part
        update_stmt = ", ".join([
            f"{col}=VALUES({col})" 
            for col in columns 
            if col not in ['game_id', 'team_id']
        ])
        
        # Create the complete INSERT query
        insert_query = f"""
        INSERT INTO nba_team_game_logs 
        ({', '.join(columns)}) 
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        {update_stmt}
        """
        
        # Convert DataFrame to list of tuples for insertion
        values = df_copy.values.tolist()
        
        # Execute insert in batches
        batch_size = 100
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            print(f"Processed batch {i//batch_size + 1} of {(len(values)-1)//batch_size + 1}")
        
        print(f"Successfully processed {len(df)} game logs")
        
    except Error as e:
        print(f"Error inserting data: {e}")
        print("Error details:", e.args)
        print("Columns in DataFrame:", df_copy.columns.tolist())
        cursor.execute("DESCRIBE nba_team_game_logs")
        print("Table structure:", cursor.fetchall())
    finally:
        if cursor:
            cursor.close()

def print_game_logs_statistics(connection):
    """Print various statistics about the game logs in the database"""
    cursor = None
    try:
        cursor = connection.cursor(buffered=True)
        
        # Get total games
        cursor.execute("SELECT COUNT(DISTINCT GAME_ID) FROM nba_team_game_logs")
        total_games = cursor.fetchone()[0]
        
        # Get date range
        cursor.execute("""
            SELECT MIN(GAME_DATE), MAX(GAME_DATE)
            FROM nba_team_game_logs
        """)
        date_range = cursor.fetchone()
        
        # Get highest scoring game
        cursor.execute("""
            SELECT TEAM_NAME, MATCHUP, GAME_DATE, PTS
            FROM nba_team_game_logs
            ORDER BY PTS DESC
            LIMIT 1
        """)
        highest_scoring = cursor.fetchone()
        
        # Print statistics
        print("\nGame Logs Statistics:")
        print(f"Total games recorded: {total_games}")
        print(f"Date range: {date_range[0]} to {date_range[1]}")
        if highest_scoring:
            print(f"\nHighest scoring game:")
            print(f"{highest_scoring[0]} ({highest_scoring[1]}) on {highest_scoring[2]}: {highest_scoring[3]} points")
        
    except Error as e:
        print(f"Error getting statistics: {e}")
    finally:
        if cursor:
            cursor.close()



def run_nba_team_game_logs():
    connection = None
    try:
        # Connect to MySQL
        connection = connect_to_mysql()
        if not connection:
            print("Failed to establish database connection")
            return
        
        # Get all NBA teams
        try:
            nba_teams = teams.get_teams()
        except Exception as e:
            print(f"Error fetching NBA teams: {e}")
            return
            
        # Fetch and insert game logs for each team
        for team in nba_teams:
            print(f"\nProcessing game logs for {team['full_name']}...")
            game_logs_df = fetch_team_game_logs(team['id'])
            
            if game_logs_df is not None and not game_logs_df.empty:
                insert_game_logs(connection, game_logs_df)
        
        # Print statistics
        print_game_logs_statistics(connection)
            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("\nMySQL connection closed")

if __name__ == "__main__":
    run_nba_team_game_logs()