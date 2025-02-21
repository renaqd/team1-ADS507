import streamlit as st
import pandas as pd
import plotly.express as px

conn = st.connection("sql", type="sql", driver="pymysql")

def get_player_stats():
    df = conn.query(
        """
        SELECT p.full_name as player_name, t.team_abbreviation,
               AVG(h.pts) as avg_points, AVG(h.contested_shots) as avg_contested_shots,
               AVG(h.deflections) as avg_deflections, AVG(h.charges_drawn) as avg_charges_drawn,
               AVG(h.screen_assists) as avg_screen_assists, AVG(h.loose_balls_recovered) as avg_loose_balls_recovered
        FROM players p
        JOIN hustle_stats h ON p.player_id = h.player_id
        JOIN teams t ON p.team_id = t.team_id
        GROUP BY p.player_id
        """
    )
    return df

def run():
    st.title("NBA Player Comparison Dashboard")

    df = get_player_stats()

    if df.empty:
        st.error("Failed to load data. Please check your database connection.")
        return

    # Player filter
    all_players = df['player_name'].tolist()
    selected_players = st.multiselect("Select players to compare", all_players, default=all_players[:5])
    filtered_df = df[df['player_name'].isin(selected_players)]

    # Stat selection
    stats = ['avg_points', 'avg_contested_shots', 'avg_deflections', 'avg_charges_drawn', 'avg_screen_assists', 'avg_loose_balls_recovered']
    selected_stat = st.selectbox("Select Stat to Compare", stats)

    # Visualization
    st.subheader(f"Player Comparison: {selected_stat.replace('avg_', '').replace('_', ' ').title()}")
    fig = px.bar(filtered_df, x='player_name', y=selected_stat, color='team_abbreviation',
                 labels={selected_stat: selected_stat.replace('avg_', '').replace('_', ' ').title(), 
                         'player_name': 'Player'})
    st.plotly_chart(fig)

    # Scatter plot
    st.subheader("Points vs Contested Shots")
    fig = px.scatter(filtered_df, x='avg_contested_shots', y='avg_points', 
                     color='team_abbreviation', hover_name='player_name',
                     labels={'avg_contested_shots': 'Avg Contested Shots', 'avg_points': 'Avg Points'})
    st.plotly_chart(fig)

    # Raw data display
    if st.checkbox("Show Raw Data"):
        st.write(filtered_df)

if __name__ == "__main__":
    run()