import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="WARN Tracker: From Scratch", layout="wide")

# Point to the file your GitHub Action just created
DATA_URL = "https://raw.githubusercontent.com/YOUR_USER/YOUR_REPO/main/data/processed/consolidated.csv"

@st.cache_data
def load_data():
    df = pd.read_csv(DATA_URL)
    # The 'warn-transformer' tool standardizes column names for us
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    return df

st.title("📊 National WARN Data (Scratch Build)")

try:
    df = load_data()
    
    # Visualization: Heatmap
    state_counts = df.groupby('state')['workers'].sum().reset_index()
    fig = px.choropleth(state_counts, locations='state', locationmode="USA-states", 
                        color='workers', scope="usa", color_continuous_scale="Reds")
    st.plotly_chart(fig, use_container_width=True)

    # Searchable Data
    st.dataframe(df[['state', 'company', 'workers', 'date']].sort_values('date', ascending=False))

except Exception as e:
    st.info("Waiting for first data build... Run the GitHub Action manually to start.")
