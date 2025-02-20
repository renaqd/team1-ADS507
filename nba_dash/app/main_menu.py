import streamlit as st
import team_dashboard
import player_dashboard

def main():
    st.title("NBA Hustle Stats Dashboard")
    
    menu = ["Team Comparison", "Player Comparison"]
    choice = st.sidebar.selectbox("Select Dashboard", menu)
    
    if choice == "Team Comparison":
        team_dashboard.run()
    elif choice == "Player Comparison":
        player_dashboard.run()

if __name__ == "__main__":
    main()