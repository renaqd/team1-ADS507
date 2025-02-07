import logging
from config import connect_to_mysql
from setup_db import create_database, create_tables
import fetch_teams
import fetch_players
import nba_team_game_logs
import player_advanced_stats
import hustle_stats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Execute all database setup and data fetching operations."""
    connection = None
    try:
        # Step 1: Create database and tables
        logger.info("Setting up database and tables...")
        create_database()
        create_tables()
        logger.info("Database setup complete!")

        # Establish database connection
        connection = connect_to_mysql()
        if not connection:
            logger.error("Failed to connect to the database.")
            return

        # Step 2: Fetch and insert teams data
        logger.info("Fetching teams data...")
        fetch_teams.run_fetch_teams()
        logger.info("Teams data fetch complete!")

        # Step 3: Fetch and insert players data
        logger.info("Fetching players data...")
        fetch_players.run_fetch_players()
        logger.info("Players data fetch complete!")


        
        # Step 3.5 Fetch and insert nba team game log
        logger.info("Fetching game log data...")
        nba_team_game_logs.run_nba_team_game_logs()
        logger.info("Game log data fetch complete!")

        # Step 4: Fetch hustle data
        logger.info("Fetching hustle stats...")
        hustle_stats.run_fetch_hustle_stats()
        logger.info("Hustle stats fetch complete!")



        # Step 4: Fetch and insert advanced stats
        # logger.info("Fetching advanced stats...")
        # player_advanced_stats.run_fetch_advanced_stats()
        # logger.info("Advanced stats fetch complete!")

        logger.info("All operations completed successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        if connection and connection.is_connected():
            connection.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()