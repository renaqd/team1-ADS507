# Insert Data onto Local MySQL and Run Dashboard

This python script inserts data from nba_api to your local mysql server.

| Statement         | Output                                         |
|------------------|-----------------------------------------------|
| `show databases;` | nba_db                                   |
| `show tables;`   | teams<br>players<br>hustle_stats |


## Step 1: Enter MySQL db connection credentials 

    1. Modify `template.env` file and Enter user, pw, and host
    2. Rename rename template.env file to `.env`

## Step 2: Navigate to the "team1-ADS507" folder in command line

* activate your environment with conda

or 

* Create a virtual environment
```
python -m venv .venv
```

**To activate your virtual environment:**

> For Windows:
> ```
> .\.venv\Scripts\activate
> ```

> For macOS/Linux
> ```
> source .venv/bin/activate
> ```

## Step 3: Install dependencies
```
pip install -r requirements.txt
```

## Step 4: Navigate to "team1-ADS507/nba_dash" and run `main.py` script
```
python main.py
```
Enter :
> '1' for initial setup (create database and insert backup db)
> 
> '2' for update (fetch new data onto existing db)
> 
> '3' to start scheduler (update to run every day at 2:00 AM)

## Step 5: Navigate to "team1-ADS507/nba_dash/app/.streamlit" and enter db connection credentials 

    1. Modify `template_secrets.toml` file and complete the following (same as .env):
```
[connections.sql]
dialect = "mysql"
host = ""
username = ""
password = ""
driver = "pymysql"
database = "nba_db"
```
    2. Rename rename "template_secrets.toml" file to `secrets.toml`

## Step 6: Navigae to "team1-ADS507/nba_dash/app" and run `main_menu.py` script
```
streamlit run main_menu.py
```