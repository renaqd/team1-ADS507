from config import connect_to_mysql, execute_query

def create_database():
    """Create the nba_db2 database"""
    try:
        # Connect without specifying database
        connection = connect_to_mysql(use_database=False)
        if not connection:
            return
        
        # Create database if it doesn't exist
        execute_query(connection, "CREATE DATABASE IF NOT EXISTS nba_db2")
        print("Database 'nba_db2' created or already exists")
        
        # Switch to the new database
        execute_query(connection, "USE nba_db2")
        
    finally:
        if connection and connection.is_connected():
            connection.close()

def create_tables():
    """Create all necessary tables"""
    connection = None
    try:
        # Now connect with database specified
        connection = connect_to_mysql(use_database=True)
        if not connection:
            return

        # Create players table
        create_players_table = """
        CREATE TABLE IF NOT EXISTS nba_players (
            id INT PRIMARY KEY,
            full_name VARCHAR(255),
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            is_active BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """
        
        # Create teams table
        create_teams_table = """
        CREATE TABLE IF NOT EXISTS nba_teams (
            id INT PRIMARY KEY,
            full_name VARCHAR(255),
            abbreviation VARCHAR(10),
            nickname VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(255),
            year_founded INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """

        # Create teams table
        create_game_logs_table = """
        CREATE TABLE IF NOT EXISTS nba_team_game_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            SEASON_YEAR VARCHAR(10),
            TEAM_ID INT,
            TEAM_ABBREVIATION VARCHAR(10),
            TEAM_NAME VARCHAR(255),
            GAME_ID VARCHAR(20),
            GAME_DATE DATE,
            MATCHUP VARCHAR(50),
            WL VARCHAR(1),
            MIN INT,
            FGM INT,
            FGA INT,
            FG_PCT FLOAT,
            FG3M INT,
            FG3A INT,
            FG3_PCT FLOAT,
            FTM INT,
            FTA INT,
            FT_PCT FLOAT,
            OREB INT,
            DREB INT,
            REB INT,
            AST INT,
            TOV INT,
            STL INT,
            BLK INT,
            BLKA INT,
            PF INT,
            PFD INT,
            PTS INT,
            PLUS_MINUS INT,
            GP_RANK INT,
            W_RANK INT,
            L_RANK INT,
            W_PCT_RANK INT,
            MIN_RANK INT,
            FGM_RANK INT,
            FGA_RANK INT,
            FG_PCT_RANK INT,
            FG3M_RANK INT,
            FG3A_RANK INT,
            FG3_PCT_RANK INT,
            FTM_RANK INT,
            FTA_RANK INT,
            FT_PCT_RANK INT,
            OREB_RANK INT,
            DREB_RANK INT,
            REB_RANK INT,
            AST_RANK INT,
            TOV_RANK INT,
            STL_RANK INT,
            BLK_RANK INT,
            BLKA_RANK INT,
            PF_RANK INT,
            PFD_RANK INT,
            PTS_RANK INT,
            PLUS_MINUS_RANK INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_game (GAME_ID, TEAM_ID)
        )
        """
        
        # Create player stats table
        create_player_stats_table = """
        CREATE TABLE IF NOT EXISTS player_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            game_id VARCHAR(10),
            team_id INT,
            person_id INT,
            minutes VARCHAR(10),
            offensive_rating FLOAT,
            defensive_rating FLOAT,
            net_rating FLOAT,
            assist_percentage FLOAT,
            rebound_percentage FLOAT,
            usage_percentage FLOAT,
            UNIQUE KEY unique_player_game (game_id, person_id)
        )
        """

        create_hustle_player_stats_table = """
        CREATE TABLE IF NOT EXISTS hustle_player_stats (
            id INT AUTO_INCREMENT PRIMARY KEY,
            GAME_ID VARCHAR(10),
            TEAM_ID INT,
            TEAM_ABBREVIATION VARCHAR(10),
            TEAM_CITY VARCHAR(255),
            PLAYER_ID INT,
            PLAYER_NAME VARCHAR(255),
            START_POSITION VARCHAR(5),
            COMMENT TEXT,
            MINUTES VARCHAR(10),
            PTS INT,
            CONTESTED_SHOTS INT,
            CONTESTED_SHOTS_2PT INT,
            CONTESTED_SHOTS_3PT INT,
            DEFLECTIONS INT,
            CHARGES_DRAWN INT,
            SCREEN_ASSISTS INT,
            SCREEN_AST_PTS INT,
            OFF_LOOSE_BALLS_RECOVERED INT,
            DEF_LOOSE_BALLS_RECOVERED INT,
            LOOSE_BALLS_RECOVERED INT,
            OFF_BOXOUTS INT,
            DEF_BOXOUTS INT,
            BOX_OUT_PLAYER_TEAM_REBS INT,
            BOX_OUT_PLAYER_REBS INT,
            BOX_OUTS INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_player_game (GAME_ID, PLAYER_ID)
        )
        """
        
        # Execute all creation queries
        execute_query(connection, create_players_table)
        execute_query(connection, create_teams_table)
        execute_query(connection, create_game_logs_table)
        execute_query(connection, create_player_stats_table)
        execute_query(connection, create_hustle_player_stats_table)
        
        print("All tables created successfully")
        
    finally:
        if connection and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    create_database()
    create_tables()
