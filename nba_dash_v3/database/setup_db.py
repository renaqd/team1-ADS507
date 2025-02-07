from database.config import connect_to_mysql, execute_query

# Rest of the code remains the same

def create_database():
    """Create the nba_db database"""
    connection = None
    try:
        connection = connect_to_mysql(use_database=False)
        if not connection:
            return
        
        execute_query(connection, "CREATE DATABASE IF NOT EXISTS nba_db")
        print("Database 'nba_db' created or already exists")
        
        execute_query(connection, "USE nba_db")
        
    finally:
        if connection and connection.is_connected():
            connection.close()

def create_tables():
    """Creates necessary tables for the NBA project."""
    connection = None
    try:
        connection = connect_to_mysql()
        queries = [
            """
            CREATE TABLE IF NOT EXISTS players (
                player_id INT PRIMARY KEY,
                full_name VARCHAR(255),
                position VARCHAR(255),
                team_id INT,
                team_name VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS teams (
                team_id INT PRIMARY KEY,
                season_year VARCHAR(10),
                team_city VARCHAR(255),
                team_name VARCHAR(255),
                team_abbreviation VARCHAR(10),
                team_conference VARCHAR(10),
                wins INT,
                losses INT,
                win_pct FLOAT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS hustle_stats (
                game_id INT,
                player_id INT,
                game_date DATE,
                matchup VARCHAR(255),
                minutes INT,
                pts INT,
                contested_shots INT,
                contested_shots_2pt INT,
                contested_shots_3pt INT,
                deflections INT,
                charges_drawn INT,
                screen_assists INT,
                screen_ast_pts INT,
                off_loose_balls_recovered INT,
                def_loose_balls_recovered INT,
                loose_balls_recovered INT,
                off_boxouts INT,
                def_boxouts INT,
                boxouts INT,
                PRIMARY KEY (game_id, player_id),
                FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
            )
            """
        ]
        for query in queries:
            execute_query(connection, query)
        print("Tables created successfully")
    finally:
        if connection and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    create_database()
    create_tables()
    print("NBA database and tables created successfully")