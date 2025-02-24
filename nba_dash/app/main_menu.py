import streamlit as st
import team_dashboard
import player_dashboard
import dashboard

def main():
    #st.title("NBA Hustle Stats Dashboard")
    
    menu = ["Team Comparison", "Player Comparison", "Player 2"]
    choice = st.sidebar.selectbox("Select Dashboard", menu)
    
    if choice == "Team Comparison":
        team_dashboard.run()
    elif choice == "Player Comparison":
        player_dashboard.run()
    elif choice == "Player 2":
        dashboard.run()


if __name__ == "__main__":
    main()