import streamlit as st
import pandas as pd
import numpy as np

### Database configuration

st.set_page_config(
    page_title="NBA Hustle Dashboard",
    page_icon=":basketball:"
    )

conn = st.connection("sql", type = "sql")

st.title(':basketball: NBA Hustle Dashboard')

st.header("Player Stats")

df = conn.query("SELECT * FROM players")
st.dataframe(df)