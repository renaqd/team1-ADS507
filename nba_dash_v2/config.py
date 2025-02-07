import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
}

def connect_to_mysql(use_database=True):
    """
    Establish connection to MySQL database
    Args:
        use_database (bool): If True, connects to nba_season database. 
                           If False, connects without specifying database.
    """
    try:
        config = DB_CONFIG.copy()
        if use_database:
            config['database'] = 'nba_1'
            
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            print("Successfully connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def execute_query(connection, query, params=None, fetch=False):
    cursor = None
    try:
        cursor = connection.cursor(buffered=True)
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        if fetch:
            result = cursor.fetchall()
        else:
            connection.commit()
            result = cursor.rowcount
        return result
    finally:
        if cursor:
            cursor.close()
