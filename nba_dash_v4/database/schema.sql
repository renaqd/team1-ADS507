-- Create the database
CREATE DATABASE IF NOT EXISTS nba_db;

-- Select database
USE nba_db;

-- Create teams table
CREATE TABLE IF NOT EXISTS teams (
    team_id INT PRIMARY KEY,
    season_year VARCHAR(10),
    team_city VARCHAR(255),
    team_name VARCHAR(255),
    team_abbreviation VARCHAR(10),
    team_conference VARCHAR(10),
    wins INT,
    losses INT,
    win_pct FLOAT
);

-- Create players table
CREATE TABLE IF NOT EXISTS players (
    player_id INT PRIMARY KEY,
    full_name VARCHAR(255),
    position VARCHAR(255),
    team_id INT,
    FOREIGN KEY (team_id) REFERENCES teams(team_id) ON DELETE SET NULL
);

-- Create hustle_stats table
CREATE TABLE IF NOT EXISTS hustle_stats (
    game_id INT,
    team_id INT,
    player_id INT,
    game_date DATE,
    matchup VARCHAR(255),
    minutes INT,
    pts INT,
    contested_shots INT,
    contested_shots_2pt INT,
    contested_shots_3pt INT,
    deflections INT,
    charges_drawn INT,
    screen_assists INT,
    screen_ast_pts INT,
    off_loose_balls_recovered INT,
    def_loose_balls_recovered INT,
    loose_balls_recovered INT,
    off_boxouts INT,
    def_boxouts INT,
    boxouts INT,
    PRIMARY KEY (game_id, player_id),
    INDEX idx_hustle_game (game_id),
    INDEX idx_hustle_team (team_id),
);

-- Insert the Free Agents team
INSERT IGNORE INTO teams (team_id, season_year, team_city, team_name, team_abbreviation, team_conference, wins, losses, win_pct)
VALUES (0, "2024-25", 'Free Agents', 'Free Agents', 'FA', 'FA', 0, 0, 0.0);