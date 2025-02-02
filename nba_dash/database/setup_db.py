import pymysql
from config import get_db_connection

def create_database():
    """Creates the NBA database if it doesn't exist."""
    connection = get_db_connection()

    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS nba_db")
    cursor.close()
    connection.close()

def create_tables():
    """Creates necessary tables for the NBA project."""
    connection = get_db_connection()
    cursor = connection.cursor()

    queries = [
        """
        CREATE TABLE IF NOT EXISTS players (
            player_id INT PRIMARY KEY, -- player_id and person_id are the same
            full_name VARCHAR(255), -- get from commonplayerinfo as display_first_last
            position VARCHAR(255), -- get from commonplayerinfo as position
            team_id INT, -- get from commonplayerinfo as team_id
            team_name VARCHAR(255), -- get from commonplayerinfo as team_name
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS teams (
            team_id INT PRIMARY KEY,
            season_year VARCHAR(10),
            team_city VARCHAR(255),
            team_name VARCHAR(255)
            team_abbreviation VARCHAR(10),
            team_conference VARCHAR(10),
            wins INT,
            losses INT,
            win_pct FLOAT,
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
        ### Insert view query here when ready....
    ]

    for query in queries:
        cursor.execute(query)

    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    create_database()
    create_tables()
    print("NBA database and tables created successfully")
