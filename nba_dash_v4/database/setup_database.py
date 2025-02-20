import os
import logging
from database.config import get_db_connection, execute_query

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_database():
    try:
        # Get the current directory (where setup_database.py is located)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Construct the path to schema.sql
        schema_path = os.path.join(current_dir, 'schema.sql')
        
        # Read the schema file
        with open(schema_path, 'r') as schema_file:
            schema_script = schema_file.read()
        
        # Connect to the database
        connection = get_db_connection()
        if not connection:
            raise Exception("Failed to connect to the database")

        # Execute the schema script
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