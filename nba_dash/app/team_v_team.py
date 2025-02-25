import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go


### Data loading
def run():
    conn = st.connection("sql", type = "sql")

    df = conn.query("SELECT * FROM team_stats")

    # Functions

    def get_team_image_url(team_id):
        return f"https://cdn.nba.com/logos/nba/{team_id}/primary/L/logo.svg"

    ### Page header
    st.title(':basketball::runner: NBA Hustle Dashboard')
    header = '''This dashboard allows you to compare hustle stats between two NBA teams.  
    Start by selecting two players from the dropdowns below.'''
    st.markdown(header)

    ### Page layout

    col1, col2 = st.columns(2)

    teams = df["team_name"].tolist()
    default_team_1 = "Lakers"
    default_team_2 = "Warriors"

    with col1:
        selected_team_1 = st.selectbox("Select Team", teams, index=teams.index(default_team_1), key="player_1")
        team_id_1 = df[df["team_name"] == selected_team_1]["team_id"].values[0]
        st.image(get_team_image_url(team_id_1), width = 100)


    with col2:
        selected_team_2 = st.selectbox("Select Team", teams, index=teams.index(default_team_2), key="player_2")
        team_id_2 = df[df["team_name"] == selected_team_2]["team_id"].values[0]
        st.image(get_team_image_url(team_id_2), width = 100)


    ### Player stats comparison chart
    container = st.container()

    with container:

        categories = ['Points Contested Shots 3pt', 'Deflections', 'Boxouts', 'Screen Assists', 'Loose Balls Recovered']

        fig = go.Figure()

        fig.add_trace(go.Scatterpolar(
            r=df[df["team_id"] == team_id_1][categories].values[0],
            theta=categories,
            fill='toself',
            fillcolor='rgba(0, 74, 255, 0.5)',
            name=selected_team_1
        ))
        fig.add_trace(go.Scatterpolar(
            r=df[df["team_id"] == team_id_2][categories].values[0],
            theta=categories,
            fill='toself',
            fillcolor='rgba(255, 0, 0, 0.5)',
            name=selected_team_2
        ))

        fig.update_layout(
        polar=dict(
            radialaxis=dict(
            visible=True,
            range=[0, df[df["team_id"].isin([team_id_1, team_id_2])][categories].max().max()]
            )),
        showlegend=False
        )

        st.plotly_chart(fig, use_container_width=True)

    ### Player stats comparison

    team_1_df = df[df["team_id"] == team_id_1].drop(columns=["team_id", "team_name"]).T.reset_index()
    team_1_df.columns = ["Per Game Stats", "Value_P1"]

    team_2_df = df[df["team_id"] == team_id_2].drop(columns=["team_id", "team_name"]).T.reset_index()
    team_2_df.columns = ["Per Game Stats", "Value_P2"]

    # Merge DataFrames for side-by-side comparison
    comparison_df = team_1_df.merge(team_2_df, on="Per Game Stats")
    comparison_df = comparison_df[['Value_P1', 'Per Game Stats', 'Value_P2']]

    # Apply conditional formatting
    def highlight_stats(row):
        """Highlight green if Player 1 stat is higher, red if lower"""
        color1 = "background-color: #d4edda; color: #155724;" if row["Value_P1"] > row["Value_P2"] else "background-color: #f8d7da; color: #721c24;" if row["Value_P1"] < row["Value_P2"] else ""
        color2 = "background-color: #d4edda; color: #155724;" if row["Value_P2"] > row["Value_P1"] else "background-color: #f8d7da; color: #721c24;" if row["Value_P2"] < row["Value_P1"] else ""
        return [color1] + [""] + [color2]

    styled_df = comparison_df.style.apply(highlight_stats, axis=1)

    column_config={
            "Value_P1": st.column_config.NumberColumn(
                selected_team_1,
                format="%.1f"
            ),

            "Per Game Stats": st.column_config.TextColumn(
                "Stat (Per Game)"
            ),
            "Value_P2": st.column_config.NumberColumn(
                selected_team_2,
                format="%.1f"
            )
    }

    st.dataframe(styled_df, column_config = column_config, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    run()