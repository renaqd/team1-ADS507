import streamlit as st
import pandas as pd
import plotly.express as px


def get_team_stats():
    conn = conn = st.connection("sql", type="sql", driver="pymysql")

    df = conn.query(
        """
        SELECT t.team_name, t.team_abbreviation, t.wins, t.losses, t.win_pct,
               SUM(h.deflections) as total_deflections,
               SUM(h.charges_drawn) as total_charges_drawn,
               SUM(h.screen_assists) as total_screen_assists,
               SUM(h.loose_balls_recovered) as total_loose_balls_recovered
        FROM teams t
        JOIN hustle_stats h ON t.team_id = h.team_id
        GROUP BY t.team_id
        """
    )
     
    return df

def run():
    st.title("NBA Team Dashboard")
    st.markdown("This dashboard allows you to compare hustle stats between NBA teams.")

    df = get_team_stats()

    if df.empty:
        st.error("Failed to load data. Please check your database connection.")
        return

    # Team Win Percentage
    st.subheader("Team Win Percentage")
    win_bar = df.sort_values(by='win_pct', ascending=False)
    fig = px.bar(win_bar, x='team_abbreviation', y='win_pct', color='team_name',
                 labels={'win_pct': 'Win Percentage', 'team_abbreviation': 'Team'},
                 hover_data=['wins', 'losses'])
    st.plotly_chart(fig)

    # Hustle Stats Comparison
    st.subheader("Team Hustle Stats Comparison")
    hustle_stats = ['total_deflections', 'total_charges_drawn', 'total_screen_assists', 'total_loose_balls_recovered']
    selected_stat = st.selectbox("Select Hustle Stat", hustle_stats)
    
    stat_bar = df.sort_values(by=selected_stat, ascending=False)
    fig = px.bar(stat_bar, x='team_abbreviation', y=selected_stat, color='team_name',
                 labels={selected_stat: selected_stat.replace('total_', '').replace('_', ' ').title(), 
                         'team_abbreviation': 'Team'})
    st.plotly_chart(fig)

    # Raw data display
    if st.checkbox("Show Raw Data"):
        st.write(stat_bar)

if __name__ == "__main__":
    run()