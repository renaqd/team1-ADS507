import streamlit as st
import pandas as pd
import numpy as np
from mysql.connector import Error
import mysql.connector 
import os
from dotenv import load_dotenv

load_dotenv()

### Database configuration

DB_CONFIG = {
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
}

def connect_to_mysql(use_database=True):
    """
    Establish connection to MySQL database
    Args:
        use_database (bool): If True, connects to nba_db database. 
                           If False, connects without specifying database.
    """
    try:
        config = DB_CONFIG.copy()
        if use_database:
            config['database'] = 'nba_db'
            
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            print("Successfully connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def fetch_data(query):
    conn = connect_to_mysql()
    if conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.execute(query)

            # Fetch all results
            records = cursor.fetchall()

            return records

        except Error as e:
            st.error(f"Error while connecting to MySQL: {e}")
            return None
        finally:
            if conn.is_connected():
                cursor.close() 
                conn.close()

### Streamlit Code
                
st.set_page_config(
    page_title="NBA Hustle Dashboard",
    page_icon=":basketball:"
    )

st.title(':basketball: NBA Hustle Dashboard')

st.header("Player Stats")

data = fetch_data("SELECT * FROM player_stats")

if data:
    df = pd.DataFrame(data, columns=['Name', 'Position', 'Min.', 'Pts.', 'Deflection'])  # Adjust column names
    st.dataframe(df)
else:
    st.write("No data available")



