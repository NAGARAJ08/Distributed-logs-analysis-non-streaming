import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os

st.set_page_config(page_title="Distributed Log Intelligence", layout="wide")

@st.cache_data
def load_data(metrics_path, anomalies_path):
    if not os.path.exists(metrics_path) or not os.path.exists(anomalies_path):
        st.error("Parquet data not found. Please run metrics + anomaly jobs first.")
        return None, None

    metrics_df = pd.read_parquet(metrics_path)
    anomalies_df = pd.read_parquet(anomalies_path)
    return metrics_df, anomalies_df


st.title("Distributed Log Intelligence Dashboard")

metrics_path = "../data/final/metrics/"
anomalies_path = "../data/final/anomalies/"

metrics_df, anomalies_df = load_data(metrics_path, anomalies_path)
if metrics_df is None:
    st.stop()

services = sorted(metrics_df["service"].unique())
envs = sorted(metrics_df["env"].unique())

col1, col2 = st.columns(2)
selected_service = col1.selectbox("Select Service", services)
selected_env = col2.selectbox("Select Environment", envs)

filtered_metrics = metrics_df[
    (metrics_df["service"] == selected_service) & (metrics_df["env"] == selected_env)
]
filtered_anomalies = anomalies_df[
    (anomalies_df["service"] == selected_service) & (anomalies_df["env"] == selected_env)
]

st.subheader(f"Metrics for {selected_service} ({selected_env})")

col3, col4, col5 = st.columns(3)
col3.metric("Total Logs", int(filtered_metrics["totalLogs"].sum()))
col4.metric("Avg Response Time (ms)", round(filtered_metrics["avgResponseTime"].mean(), 2))
col5.metric("Error Rate (%)", round(filtered_metrics["errorRate"].mean(), 2))

st.write("### Anomaly Trends")

if len(filtered_anomalies) > 0:
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.scatter(
        filtered_anomalies["timestamp"],
        filtered_anomalies["responseTime"],
        color="red",
        label="Anomaly",
        alpha=0.6
    )
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Response Time (ms)")
    ax.set_title(f"Anomalies in {selected_service}")
    ax.legend()
    st.pyplot(fig)
else:
    st.info("No anomalies detected for this service/environment.")

st.write("Dashboard Ready — all Spark jobs successfully completed.")
