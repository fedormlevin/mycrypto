import streamlit as st  # web development
import numpy as np  # np mean, np random
import pandas as pd  # read csv, df manipulation
import time  # to simulate a real time data, time loop
import plotly.express as px  # interactive charts


st.set_page_config(
    page_title="Real-Time Data Science Dashboard", page_icon="✅", layout="wide"
)

# dashboard title

st.title("Real-Time / Live Data Science Dashboard")

placeholder = st.empty()

with placeholder.container():
    # create three columns
    kpi1, kpi2, kpi3 = st.columns(3)

    kpi1.metric(label="Age ⏳", value=100, delta=10)
    kpi2.metric(label="Married Count 💍", value=12, delta=3)
    kpi3.metric(label="A/C Balance ＄", value=3, delta=100)
