# How to use hoopr_data_pull.py

This python script inserts data from nba_api to your local mysql server.

| Statement         | Output                                         |
|------------------|-----------------------------------------------|
| `show databases;` | nba_db                                   |
| `show tables;`   | teams<br>players<br>hustle_stats |


## Step 1: Enter MySQL db connection credentials 

* Modify template.env file and Enter user, pw, and host
* Rename rename "template.env" file to ".env"

## Step 2: Navigate to the /nba_dash_v2 folder in command line

* activate your environment with conda

or 

* Create a virtual environment
```
python -m venv .venv
```

**To activate your virtual environment:**

| Windows | macOS/Linux |
|---------|------------|
| ```.\.venv\Scripts\activate``` | ```source .venv/bin/activate``` |


## Step 3: Install dependencies
```
pip install -r requirements.txt
```

## Step 4: Run main.py script
```
python main.py
```