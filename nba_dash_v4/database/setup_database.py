import os
import logging
from database.config import connect_to_mysql, execute_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def execute_sql_file(connection, file_path):
    try:
        with open(file_path, 'r') as sql_file:
            sql_script = sql_file.read()
            for statement in sql_script.split(';'):
                if statement.strip():
                    try:
                        execute_query(connection, statement)
                    except Exception as e:
                        logger.error(f"Error executing SQL statement: {e}")
                        logger.error(f"Problematic statement: {statement}")
        logger.info(f"Successfully executed {file_path}")
    except IOError as e:
        logger.error(f"Error reading SQL file {file_path}: {e}")

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
        
        # Construct the paths to schema.sql and data.sql
        schema_path = os.path.join(current_dir, 'schema.sql')
        data_path = os.path.join(current_dir, 'data.sql')
        
        # Execute schema.sql
        execute_sql_file(connection, schema_path)
        
        # Execute data.sql if it exists
        if os.path.exists(data_path):
            execute_sql_file(connection, data_path)
        else:
            logger.info("data.sql not found. Skipping initial data population.")
        
        logger.info("Database setup completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during database setup: {e}")
        raise
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    setup_database()