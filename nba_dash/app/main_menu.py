import streamlit as st
import team_dashboard
import player_dashboard
import dashboard
import team_v_team

st.set_page_config(
    page_title="NBA Hustle Dashboard",
    page_icon=":basketball:"
)
    

def main():

    menu = ["Team Overview", "Player Overview", "Player v Player", "Team v Team"]
    choice = st.sidebar.selectbox("Select Dashboard", menu)
    
    if choice == "Team Overview":
        team_dashboard.run()
    elif choice == "Player Overview":
        player_dashboard.run()
    elif choice == "Player v Player":
        dashboard.run()
    elif choice == "Team v Team":
        team_v_team.run()


if __name__ == "__main__":
    main()