import streamlit as st
import pandas as pd

st.set_page_config(page_title="MyAirWatch", layout="wide")

st.title("MyAirWatch: Malaysia Air Quality Data Platform")

st.write("Local dashboard for Malaysia air quality analytics.")

sample = pd.DataFrame(
    {
        "state": ["Selangor", "Johor", "Penang"],
        "avg_pm25": [35.2, 28.4, 22.1],
    }
)

st.bar_chart(sample, x="state", y="avg_pm25")
