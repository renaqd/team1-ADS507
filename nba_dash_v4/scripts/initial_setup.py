import logging
import os
from database.setup_database import setup_database
from data_ingestion import run_data_ingestion

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initial_setup():
    try:
        logger.info("Running initial database setup...")
        setup_database()
        logger.info("Running initial data ingestion...")
        run_data_ingestion()
        logger.info("Initial setup completed successfully!")
    except Exception as e:
        logger.error(f"An error occurred during initial setup: {e}")

if __name__ == "__main__":
    initial_setup()