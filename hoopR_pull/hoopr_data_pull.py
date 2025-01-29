import os
from dotenv import load_dotenv
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np

load_dotenv()
db_config = {
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'host': os.environ['DB_HOST'],
    # 'raise_on_warnings': True
}

def connect_to_mysql(host, user, password):
    """Establish initial connection to MySQL server without database"""
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Successfully connected to MySQL server")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def create_database(connection, database_name):
    """Create database if it doesn't exist"""
    try:
        cursor = connection.cursor()
        
        # Create database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"Database '{database_name}' created or already exists")
        
        # Switch to the database
        cursor.execute(f"USE {database_name}")
        print(f"Using database '{database_name}'")
        
    except Error as e:
        print(f"Error creating database: {e}")
    finally:
        if cursor:
            cursor.close()

def load_hoopr_data():
    """Load data from hoopR package"""
    # Initialize R environment
    r = robjects.r

    # Install and load hoopR if not already installed
    r('''
    if (!require("hoopR")) {
        install.packages("hoopR")
    }
    library(hoopR)
    ''')
    
    # Load specific data from hoopR
    r('''
    data <- load_nba_pbp(2024)  # Example: loading play-by-play data for 2024
    ''')
    
    # Convert R dataframe to pandas dataframe
    pandas2ri.activate()
    df = robjects.globalenv['data']
    pandas_df = pandas2ri.rpy2py(df)
    
    return pandas_df

def clean_column_names(df):
    """Clean column names to be MySQL compatible"""
    # First, rename any 'id' column from the dataset to avoid conflict with primary key
    if 'id' in df.columns:
        df = df.rename(columns={'id': 'original_id'})
    
    # Replace NaN in column names with 'col_' + index
    df.columns = [f'col_{i}' if pd.isna(col) else col for i, col in enumerate(df.columns)]
    
    # Clean column names: remove special characters and spaces
    df.columns = df.columns.str.replace('[^0-9a-zA-Z_]', '_', regex=True)
    
    # Ensure column names don't start with numbers
    df.columns = [f'col_{col}' if col[0].isdigit() else col for col in df.columns]
    
    # Remove duplicate column names by adding index to duplicates
    seen = {}
    clean_cols = []
    for col in df.columns:
        # Skip renaming if it's the original 'id' column that we already renamed
        if col == 'original_id':
            clean_cols.append(col)
            continue
            
        if col in seen:
            seen[col] += 1
            clean_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            clean_cols.append(col)
    df.columns = clean_cols
    
    return df

def convert_datetime_columns(df):
    """Convert timezone-aware datetime columns to naive UTC datetime"""
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # Convert to naive UTC datetime
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def insert_data_to_mysql(connection, df, table_name):
    """Insert pandas dataframe into MySQL table"""
    try:
        # Clean column names
        df = clean_column_names(df)
        # Convert datetime columns
        df = convert_datetime_columns(df)
        
        # Replace NaN values with None (NULL in MySQL)
        df = df.replace({np.nan: None})
        
        cursor = connection.cursor()
        
        # Drop the existing table if it exists
        drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(drop_table_query)
        
        # Create column definitions
        column_definitions = []
        for col in df.columns:
            dtype = df[col].dtype
            if np.issubdtype(dtype, np.number):
                if np.issubdtype(dtype, np.integer):
                    col_type = "INT"
                else:
                    col_type = "DOUBLE"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                col_type = "DATETIME"
            else:
                max_length = df[col].dropna().astype(str).str.len().max()
                if pd.isna(max_length):
                    max_length = 255
                else:
                    max_length = max(max_length, 10)
                col_type = f"VARCHAR({max_length})"
            
            column_definitions.append(f"`{col}` {col_type}")
        
        # Create new table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_definitions)}
        )
        """
        cursor.execute(create_table_query)
        
        # Prepare insert query
        columns = df.columns.tolist()
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"""
        INSERT INTO {table_name} 
        ({', '.join([f'`{col}`' for col in columns])}) 
        VALUES ({placeholders})
        """
        
        # Convert dataframe to list of tuples for insertion
        values = df.values.tolist()
        
        # Execute insert in batches
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            print(f"Inserted batch {i//batch_size + 1} of {(len(values)-1)//batch_size + 1}")
        
        print(f"Successfully inserted {len(df)} rows into {table_name}")
        
    except Error as e:
        print(f"Error inserting data: {e}")
        print("Error details:", e.args)
        try:
            cursor.execute(f"DESCRIBE {table_name}")
            print("Table structure:", cursor.fetchall())
        except Error as e2:
            print(f"Error getting table structure: {e2}")
    finally:
        if cursor:
            cursor.close()

def main():
    database_name = 'basketball_stats'  # Name for your new database
    
    # Connect to MySQL server (without database)
    connection = connect_to_mysql(**db_config)
    if not connection:
        return
    
    try:
        # Create and use database
        create_database(connection, database_name)
        
        # Load data from hoopR
        data = load_hoopr_data()
        # Print column names before cleaning
        print("Original columns:", data.columns.tolist())
        
        # Clean column names and print
        cleaned_data = clean_column_names(data.copy())
        print("Cleaned columns:", cleaned_data.columns.tolist())
        
        # Insert data into MySQL
        insert_data_to_mysql(connection, cleaned_data, 'basketball_data')
        
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection closed")

if __name__ == "__main__":
    main()