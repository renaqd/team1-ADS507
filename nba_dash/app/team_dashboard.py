import streamlit as st
import pandas as pd
import plotly.express as px
from database_utils import get_database_connection

@st.cache_data(ttl=600)
def get_team_stats():
    conn = get_database_connection()
    if conn:
        query = """
        SELECT t.team_name, t.team_abbreviation, t.wins, t.losses, t.win_pct,
               SUM(h.deflections) as total_deflections,
               SUM(h.charges_drawn) as total_charges_drawn,
               SUM(h.screen_assists) as total_screen_assists,
               SUM(h.loose_balls_recovered) as total_loose_balls_recovered
        FROM teams t
        JOIN hustle_stats h ON t.team_id = h.team_id
        GROUP BY t.team_id
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    return pd.DataFrame()

def run():
    st.title("NBA Team Comparison Dashboard")

    df = get_team_stats()

    if df.empty:
        st.error("Failed to load data. Please check your database connection.")
        return

    # Team Win Percentage
    st.subheader("Team Win Percentage")
    fig = px.bar(df, x='team_abbreviation', y='win_pct', color='team_name',
                 labels={'win_pct': 'Win Percentage', 'team_abbreviation': 'Team'},
                 hover_data=['wins', 'losses'])
    st.plotly_chart(fig)

    # Hustle Stats Comparison
    st.subheader("Team Hustle Stats Comparison")
    hustle_stats = ['total_deflections', 'total_charges_drawn', 'total_screen_assists', 'total_loose_balls_recovered']
    selected_stat = st.selectbox("Select Hustle Stat", hustle_stats)
    
    fig = px.bar(df, x='team_abbreviation', y=selected_stat, color='team_name',
                 labels={selected_stat: selected_stat.replace('total_', '').replace('_', ' ').title(), 
                         'team_abbreviation': 'Team'})
    st.plotly_chart(fig)

    # Raw data display
    if st.checkbox("Show Raw Data"):
        st.write(df)

if __name__ == "__main__":
    run()