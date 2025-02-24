# team1-ADS507
Practical Data Engineering - Final Team Project

### Data Source
https://github.com/swar/nba_api (API Client for www.nba.com)
> Datasets
- HustleStatsBoxScore
- teams
- players


## Overview

This project is a comprehensive NBA statistics analysis system with a focus on hustle stats. Below is an overview of its main components:

### Database Configuration and Setup
- The project uses **MySQL** as the database.
- Environment variables are used for database credentials.
- A setup script is available to create and populate the database.

### Data Ingestion
- The system fetches data from the **NBA API** for players, teams, and hustle stats.
- It utilizes the `nba_api` library to interact with the NBA's official API.
- Includes retry mechanisms and error handling for API requests.

### Data Models
- The database schema includes tables for **players**, **teams**, and **hustle stats**.

### Scripts
- Contains scripts for **initial setup** and **data updates**.
- The `main.py` file allows users to choose between **initial setup** and **updates**.

### Web Application
- A **Streamlit-based web application** is included for data visualization.
- Features separate dashboards for **team and player comparisons**.
- Uses **Plotly** for creating interactive charts.

### Error Handling and Logging
- The code includes extensive **error handling** and **logging** throughout.

### Performance Considerations
- Implements **caching** for database queries to improve performance.

### Security
- Database credentials are stored in **environment variables**.
- SQL queries use **parameterized inputs** to prevent SQL injection.


## Contents
```plaintext
team1-ADS507/nba_dash/ 
├── database/
│   ├── __init__.py
│   ├── config.py
│   ├── setup_database.py
│   ├── schema.sql
│   └── data.sql
├── data_ingestion/
│   ├── __init__.py
│   ├── fetch_hustle_stats.py
│   ├── fetch_players.py
│   └── fetch_teams.py
├── scripts/
│   ├── __init__.py
│   ├── initial_setup.py
│   └── update.py
├── app/
│   ├── __init__.py
│   ├── database_utils.py
│   ├── monitoring_dashboard.py
│   ├── main_menu.py
│   ├── team_dashboard.py
│   └── player_dashboard.py
├── main.py
└── requirements.txt
```


## Pipeline Deployment
![Pipeline Deployment](imgs/pipeline.PNG)

## Pipeline Monitoring
The project implements basic monitoring through logging.

**Logging:**
- The project uses Python's built-in `logging` module.
- Logging is set up in most files using:
  ```python
  logging.basicConfig(level=logging.INFO)
- Various informational and error messages are logged throughout the code.

**Console Output:**
- Many operations print status messages to the console.

**Error Handling:**
- Try-except blocks are used to catch and log errors.

**Database Feedback:**
- Some functions return the number of rows affected by database operations.