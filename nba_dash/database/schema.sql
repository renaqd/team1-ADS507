-- Create the database
-- CREATE DATABASE IF NOT EXISTS nba_db;

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
    INDEX idx_hustle_team (team_id)
);

-- Insert the Free Agents team
INSERT IGNORE INTO teams (team_id, season_year, team_city, team_name, team_abbreviation, team_conference, wins, losses, win_pct)
VALUES (0, "2024-25", 'Free Agents', 'Free Agents', 'FA', 'FA', 0, 0, 0.0);

-- Create view for grouped player stats
create or replace view player_stats as (
    select 
        p.player_id,
        p.full_name as player_name,
        AVG(h.pts) as "Points",
        AVG(h.contested_shots) as "Contested Shots", 
        AVG(h.contested_shots_2pt) as "Points Contested Shots 2pt",
        AVG(h.contested_shots_3pt) as "Points Contested Shots 3pt",
        AVG(h.deflections) as "Deflections",
        AVG(h.charges_drawn) as "Charges Drawn",
        AVG(h.screen_assists) as "Screen Assists",
        AVG(h.screen_ast_pts) as "Screen Ast Points",
        AVG(h.off_loose_balls_recovered) as "Off Loose Balls Recovered",
        AVG(h.def_loose_balls_recovered) as "Def Loose Balls Recovered",
        AVG(h.loose_balls_recovered) as "Loose Balls Recovered",
        AVG(h.off_boxouts) as "Off Boxouts",
        AVG(h.def_boxouts) as "Def Boxouts",
        AVG(h.boxouts) as "Boxouts"
    from hustle_stats h
    join players p on h.player_id = p.player_id
    group by p.player_id, p.full_name
)


-- Create view for grouped team stats
CREATE OR REPLACE VIEW team_stats AS
SELECT 
    t.team_id AS team_id,
    t.team_name AS team_name,
    AVG(pts) AS "Points",
    AVG(contested_shots) AS "Contested Shots", 
    AVG(contested_shots_2pt) AS "Points Contested Shots 2pt",
    AVG(contested_shots_3pt) AS "Points Contested Shots 3pt",
    AVG(deflections) AS "Deflections",
    AVG(charges_drawn) AS "Charges Drawn",
    AVG(screen_assists) AS "Screen Assists",
    AVG(screen_ast_pts) AS "Screen Ast Points",
    AVG(off_loose_balls_recovered) AS "Off Loose Balls Recovered",
    AVG(def_loose_balls_recovered) AS "Def Loose Balls Recovered",
    AVG(loose_balls_recovered) AS "Loose Balls Recovered",
    AVG(off_boxouts) AS "Off Boxouts",
    AVG(def_boxouts) AS "Def Boxouts",
    AVG(boxouts) AS "Boxouts"
FROM (
    SELECT 
        team_id,
        SUM(pts) AS pts, 
        SUM(contested_shots) AS contested_shots,
        SUM(contested_shots_2pt) AS contested_shots_2pt,
        SUM(contested_shots_3pt) AS contested_shots_3pt,
        SUM(deflections) AS deflections,
        SUM(charges_drawn) AS charges_drawn,
        SUM(screen_assists) AS screen_assists,
        SUM(screen_ast_pts) AS screen_ast_pts,
        SUM(off_loose_balls_recovered) AS off_loose_balls_recovered,
        SUM(def_loose_balls_recovered) AS def_loose_balls_recovered,
        SUM(loose_balls_recovered) AS loose_balls_recovered,
        SUM(off_boxouts) AS off_boxouts,
        SUM(def_boxouts) AS def_boxouts,
        SUM(boxouts) AS boxouts
    FROM hustle_stats
    GROUP BY game_id, team_id 
) AS team_hustle
JOIN teams t ON team_hustle.team_id = t.team_id
GROUP BY t.team_id, t.team_name;
