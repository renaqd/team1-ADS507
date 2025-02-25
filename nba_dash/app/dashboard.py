import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

### Page configuration

#st.set_page_config(
#    page_title="NBA Hustle Dashboard",
#    page_icon=":basketball:"
#    )

### Data loading
def run():
    conn = st.connection("sql", type = "sql")

    df = conn.query("SELECT * FROM player_stats")

    # Functions

    def get_player_image_url(player_id):
        return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"

    ### Page header
    st.title(':basketball::runner: NBA Hustle Dashboard')
    header = '''This dashboard allows you to compare hustle stats between two NBA players.  
    Start by selecting two players from the dropdowns below.'''
    st.markdown(header)

    ### Page layout

    col1, col2 = st.columns(2)

    players = df["player_name"].tolist()
    default_player_1 = "LeBron James"
    default_player_2 = "Stephen Curry"

    with col1:
        selected_player_1 = st.selectbox("Select Player", players, index=players.index(default_player_1), key="player_1")
        player_id_1 = df[df["player_name"] == selected_player_1]["player_id"].values[0]
        st.image(get_player_image_url(player_id_1))


    with col2:
        selected_player_2 = st.selectbox("Select Player", players, index=players.index(default_player_2), key="player_2")
        player_id_2 = df[df["player_name"] == selected_player_2]["player_id"].values[0]
        st.image(get_player_image_url(player_id_2))


    ### Player stats comparison chart
    container = st.container()

    with container:

        categories = ['Contested Shots', 'Deflections', 'Boxouts', 'Screen Assists', 'Loose Balls Recovered']

        fig = go.Figure()

        fig.add_trace(go.Scatterpolar(
            r=df[df["player_id"] == player_id_1][categories].values[0],
            theta=categories,
            fill='toself',
            fillcolor='rgba(0, 74, 255, 0.5)',
            name=selected_player_1
        ))
        fig.add_trace(go.Scatterpolar(
            r=df[df["player_id"] == player_id_2][categories].values[0],
            theta=categories,
            fill='toself',
            fillcolor='rgba(255, 0, 0, 0.5)',
            name=selected_player_2
        ))

        fig.update_layout(
        polar=dict(
            radialaxis=dict(
            visible=True,
            range=[0, 5]
            )),
        showlegend=False
        )

        st.plotly_chart(fig, use_container_width=True)

    ### Player stats comparison

    player_1_df = df[df["player_id"] == player_id_1].drop(columns=["player_id", "player_name"]).T.reset_index()
    player_1_df.columns = ["Per Game Stats", "Value_P1"]

    player_2_df = df[df["player_id"] == player_id_2].drop(columns=["player_id", "player_name"]).T.reset_index()
    player_2_df.columns = ["Per Game Stats", "Value_P2"]

    # Merge DataFrames for side-by-side comparison
    comparison_df = player_1_df.merge(player_2_df, on="Per Game Stats")
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
                selected_player_1,
                format="%.1f"
            ),

            "Per Game Stats": st.column_config.TextColumn(
                "Stat (Per Game)"
            ),
            "Value_P2": st.column_config.NumberColumn(
                selected_player_2,
                format="%.1f"
            )
    }

    st.dataframe(styled_df, column_config = column_config, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    run()