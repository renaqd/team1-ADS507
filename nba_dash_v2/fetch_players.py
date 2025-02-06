import pandas as pd
from nba_api.stats.static import players
from config import connect_to_mysql, execute_query

def fetch_nba_players():
    """Fetch all NBA players data"""
    try:
        # Get all players using the nba_api
        all_players = players.get_players()
        
        # Convert to pandas DataFrame for easier handling
        df = pd.DataFrame(all_players)
        
        print(f"Successfully fetched {len(df)} NBA players")
        return df
        
    except Exception as e:
        print(f"Error fetching NBA players: {e}")
        return None

def insert_players_data(connection, df):
    """Insert or update players data into MySQL table"""
    try:
        cursor = connection.cursor()
        
        # Prepare insert query with ON DUPLICATE KEY UPDATE
        insert_query = """
        INSERT INTO nba_players (id, full_name, first_name, last_name, is_active)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            full_name = VALUES(full_name),
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            is_active = VALUES(is_active),
            updated_at = CURRENT_TIMESTAMP
        """
        
        # Convert DataFrame to list of tuples for insertion
        values = df[['id', 'full_name', 'first_name', 'last_name', 'is_active']].values.tolist()
        
        # Execute insert in batches
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            print(f"Processed batch {i//batch_size + 1} of {(len(values)-1)//batch_size + 1}")
        
        print(f"Successfully processed {len(df)} players")
        
    except Error as e:
        print(f"Error inserting data: {e}")
        print("Error details:", e.args)
    finally:
        if cursor:
            cursor.close()

def run_fetch_players():
    """Main function to fetch and insert NBA players data"""
    connection = None
    try:
        # Connect to MySQL
        connection = connect_to_mysql()
        if not connection:
            return

        # Fetch and insert NBA players data
        players_df = fetch_nba_players()
        if players_df is not None:
            insert_players_data(connection, players_df)
            # Print some statistics
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM nba_players")
            total_players = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM nba_players WHERE is_active = TRUE")
            active_players = cursor.fetchone()[0]
            
            print("\nDatabase Statistics:")
            print(f"Total players: {total_players}")
            print(f"Active players: {active_players}")
            print(f"Inactive players: {total_players - active_players}")
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("\nMySQL connection closed")

if __name__ == "__main__":
    run_fetch_players()