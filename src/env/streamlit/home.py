import streamlit as st  # web development
import numpy as np  # np mean, np random
import pandas as pd  # read csv, df manipulation
import time  # to simulate a real time data, time loop
import plotly.express as px  # interactive charts
import subprocess
import re
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Function to execute df -h and return its output
def get_disk_space():
    try:
        result = subprocess.run(['df', '-h'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return result.stdout
    except Exception as e:
        return f"An error occurred: {e}"

# Function to extract the Use% of /dev/vda1
def extract_use_percentage(df_output):
    lines = df_output.strip().split('\n')
    for line in lines:
        if line.startswith('/dev/vda1'):
            parts = re.split(r'\s+', line)
            return parts[4]  # Use% is the fifth column
    return "Not found"

# Display the Use% of /dev/vda1 on the Streamlit page
def display_disk_usage():
    disk_space_info = get_disk_space()
    use_percentage = extract_use_percentage(disk_space_info)
    return use_percentage


db_name = "airflow_db"
db_user = os.environ.get('postgres_user')
db_password = os.environ.get('postgres_psw')
db_host = 'localhost'
db_port = 5432

# Establish the connection
conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_password)

# Define the SQL query to select the table
sql_query = "SELECT * FROM dag_run where last_scheduling_decision >= current_date"

# Execute the query and store the result in a DataFrame
df = pd.read_sql_query(sql_query, conn)
# Close the database connection
conn.close()

dag_success_count = len(df[df['state']=='success']['dag_id'].unique())
dag_running_count = len(df[df['state']=='running']['dag_id'].unique())
dag_failed_count = len(df[~df['state'].isin(['success', 'running'])]['dag_id'].unique())


st.set_page_config(
    page_title="My Dashboard", page_icon="✅", layout="wide"
)

# dashboard title

st.title("General info")

placeholder = st.empty()

with placeholder.container():
    # create three columns
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    disk_usage = display_disk_usage()
    kpi1.metric(label="Disk Usage", value=disk_usage)
    kpi2.metric(label="Dags Failed", value=dag_failed_count)
    kpi3.metric(label="Dags Finished", value=dag_success_count)
    kpi4.metric(label="Dags Running", value=dag_running_count)



