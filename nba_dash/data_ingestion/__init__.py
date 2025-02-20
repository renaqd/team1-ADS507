from .fetch_teams import fetch_teams
from .fetch_players import fetch_players
from .fetch_hustle_stats import fetch_hustle_stats

import logging

logger = logging.getLogger(__name__)

def run_data_ingestion():
    try:
        logger.info("Fetching recent teams data...")
        fetch_teams()

        logger.info("Fetching recent players data...")
        fetch_players()

        logger.info("Fetching recent hustle stats...")
        fetch_hustle_stats()

        logger.info("Data ingestion completed successfully!")
    except Exception as e:
        logger.error(f"An error occurred during data ingestion: {e}")