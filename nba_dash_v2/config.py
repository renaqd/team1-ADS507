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
    'database': 'nba_season'
}

def connect_to_mysql():
    """Establish connection to MySQL database"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            print("Successfully connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def execute_query(connection, query, params=None, fetch=False):
    """Execute a query with proper cursor handling"""
    cursor = None
    try:
        cursor = connection.cursor(buffered=True)
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        if fetch:
            result = cursor.fetchall()
            return result
        else:
            connection.commit()
            return None
    finally:
        if cursor:
            cursor.close()
