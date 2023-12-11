import streamlit as st
import matplotlib.pyplot as plt
from clickhouse_driver import Client
import pandas as pd

# Function to get data from ClickHouse
def get_data(query):
    client = Client(host='your_clickhouse_host', user='your_user', password='your_password', database='your_database')
    return pd.DataFrame(client.execute(query))

# Streamlit UI
st.title("ClickHouse Data Visualization")

# Define your query
query = "SELECT your_column, COUNT(*) FROM your_table GROUP BY your_column"

# Fetch data
data = get_data(query)

# Plotting
st.write("Bar Chart:")
fig, ax = plt.subplots()
ax.bar(data['your_column'], data['count'])
ax.set_xlabel('Your Column')
ax.set_ylabel('Count')
ax.set_title('Bar Chart of Your Data')
st.pyplot(fig)
