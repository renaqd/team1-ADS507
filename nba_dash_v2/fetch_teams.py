import pandas as pd
from nba_api.stats.static import teams
from config import connect_to_mysql, execute_query
from mysql.connector import Error

def fetch_nba_teams():
    """Fetch all NBA teams data"""
    try:
        all_teams = teams.get_teams()
        df = pd.DataFrame(all_teams)
        print(f"Successfully fetched {len(df)} NBA teams")
        return df
    except Exception as e:
        print(f"Error fetching NBA teams: {e}")
        return None

def insert_teams_data(connection, df):
    """Insert or update teams data into MySQL table"""
    cursor = None
    try:
        cursor = connection.cursor(buffered=True)
        
        insert_query = """
        INSERT INTO nba_teams 
        (id, full_name, abbreviation, nickname, city, state, year_founded)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            full_name = VALUES(full_name),
            abbreviation = VALUES(abbreviation),
            nickname = VALUES(nickname),
            city = VALUES(city),
            state = VALUES(state),
            year_founded = VALUES(year_founded),
            updated_at = CURRENT_TIMESTAMP
        """
        
        values = df[['id', 'full_name', 'abbreviation', 'nickname', 
                    'city', 'state', 'year_founded']].values.tolist()
        
        cursor.executemany(insert_query, values)
        connection.commit()
        print(f"Successfully processed {len(df)} teams")
        
    except Error as e:
        print(f"Error inserting data: {e}")
        print("Error details:", e.args)
    finally:
        if cursor:
            cursor.close()

def print_team_statistics(connection):
    """Print various statistics about the teams in the database"""
    try:
        # Get total teams
        total_query = "SELECT COUNT(*) FROM nba_teams"
        total_teams = execute_query(connection, total_query, fetch=True)[0][0]
        
        # Get teams by state
        state_query = """
            SELECT state, COUNT(*) as count 
            FROM nba_teams 
            GROUP BY state 
            ORDER BY count DESC
        """
        teams_by_state = execute_query(connection, state_query, fetch=True)
        
        # Get oldest team
        oldest_query = """
            SELECT full_name, year_founded 
            FROM nba_teams 
            WHERE year_founded = (SELECT MIN(year_founded) FROM nba_teams)
            LIMIT 1
        """
        oldest_team = execute_query(connection, oldest_query, fetch=True)[0]
        
        # Get newest team
        newest_query = """
            SELECT full_name, year_founded 
            FROM nba_teams 
            WHERE year_founded = (SELECT MAX(year_founded) FROM nba_teams)
            LIMIT 1
        """
        newest_team = execute_query(connection, newest_query, fetch=True)[0]
        
        # Print statistics
        print("\nTeam Statistics:")
        print(f"Total teams: {total_teams}")
        
        print("\nTeams by State:")
        for state, count in teams_by_state:
            print(f"{state}: {count}")
            
        print(f"\nOldest team: {oldest_team[0]} (founded {oldest_team[1]})")
        print(f"Newest team: {newest_team[0]} (founded {newest_team[1]})")
        
    except Error as e:
        print(f"Error getting statistics: {e}")

def run_fetch_teams():
    """Main function to fetch and insert NBA teams data"""
    connection = None
    try:
        # Connect to MySQL
        connection = connect_to_mysql()
        if not connection:
            return

        # Fetch and insert NBA teams data
        teams_df = fetch_nba_teams()
        if teams_df is not None:
            insert_teams_data(connection, teams_df)
            print_team_statistics(connection)

    finally:
        if connection and connection.is_connected():
            connection.close()
            print("\nMySQL connection closed")

if __name__ == "__main__":
    run_fetch_teams()