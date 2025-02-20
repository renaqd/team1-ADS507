import logging
import os
from database.config import connect_to_mysql, execute_query
from database.setup_database import setup_database
from data_ingestion import run_data_ingestion

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def drop_database():
    connection = None
    try:
        connection = connect_to_mysql(use_database=False)
        if not connection:
            raise Exception("Failed to connect to MySQL server")

        drop_db_query = "DROP DATABASE IF EXISTS nba_db"
        execute_query(connection, drop_db_query)
        logger.info("Existing 'nba_db' database dropped successfully")
    except Exception as e:
        logger.error(f"Error dropping database: {e}")
    finally:
        if connection and connection.is_connected():
            connection.close()

def initial_setup(drop_existing=False):
    try:
        if drop_existing:
            logger.info("Dropping existing database...")
            drop_database()

        logger.info("Running initial database setup...")
        setup_database()
        logger.info("Running initial data ingestion...")
        run_data_ingestion()
        logger.info("Initial setup completed successfully!")
    except Exception as e:
        logger.error(f"An error occurred during initial setup: {e}")

if __name__ == "__main__":
    drop_existing = input("Do you want to drop the existing database? (y/n): ").lower() == 'y'
    initial_setup(drop_existing)