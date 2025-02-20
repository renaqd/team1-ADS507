import os
import logging
from database.config import connect_to_mysql, execute_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_database():
    try:
        # First, connect without specifying a database
        connection = connect_to_mysql(use_database=False)
        if not connection:
            raise Exception("Failed to connect to MySQL server")

        # Create the database if it doesn't exist
        create_db_query = "CREATE DATABASE IF NOT EXISTS nba_db"
        execute_query(connection, create_db_query)
        logger.info("Database 'nba_db' created or already exists")

        # Close the initial connection
        connection.close()

        # Now connect to the specific database
        connection = connect_to_mysql(use_database=True)
        if not connection:
            raise Exception("Failed to connect to the 'nba_db' database")

        # Get the current directory (where setup_database.py is located)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Construct the path to schema.sql
        schema_path = os.path.join(current_dir, 'schema.sql')
        
        # Read and execute the schema file
        with open(schema_path, 'r') as schema_file:
            schema_script = schema_file.read()
            for statement in schema_script.split(';'):
                if statement.strip():
                    execute_query(connection, statement)
        
        logger.info("Database setup completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during database setup: {e}")
        raise
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    setup_database()