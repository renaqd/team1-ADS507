import logging
from data_ingestion import run_data_ingestion

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_update():
    try:
        logger.info("Running data update...")
        run_data_ingestion()
        logger.info("Data update completed successfully!")
    except Exception as e:
        logger.error(f"An error occurred during update: {e}")

if __name__ == "__main__":
    run_update()