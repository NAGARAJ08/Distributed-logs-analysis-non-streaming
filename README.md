# Distributed Logs Analysis (Non-Streaming)

A **PySpark-based distributed log intelligence system** that processes, analyzes, and derives insights from large-scale log data in a **batch (non-streaming)** fashion.

This project goes beyond a simple ETL — it demonstrates true distributed data processing concepts like **RDDs**, **DataFrames**, **Window functions**, **partitioning**, and **anomaly detection**.

---

## Project Overview

Modern distributed systems generate massive logs across multiple services and environments.  
This project simulates such a system and performs the following:

1. **Generate Synthetic Logs** using Faker (partitioned by service & environment)  
2. **Compute Service-Level Metrics**
   - Average response time  
   - Error rate  
   - Total log count  
   (Implemented using both **DataFrame APIs** and **RDD transformations**)  
3. **Detect Anomalies & Spikes** in response times using z-scores over a sliding window  
4. **Store Outputs in Parquet** for efficient access  
5. **Aggregate & Visualize Insights**

---

## Tech Stack

- **Apache Spark (PySpark)** — distributed data processing  
- **Python 3.10+**  
- **Faker** — for realistic synthetic log data  
- **Pandas**, **Matplotlib**, **Seaborn** — visualization  
- **Parquet / PyArrow** — optimized data storage

---

## Project Structure

```plaintext
Distributed-logs-analysis-non-streaming/
│
├── data/
│   ├── raw_logs/           # synthetic JSON logs (input)
│   ├── metrics/            # computed metrics (output)
│   ├── anomalies/          # anomaly records (output)
│   └── enriched/           # enriched logs with z-scores
│
├── scripts/
│   ├── generate_logs.py          # Step 1 - log generation
│   ├── compute_metrics.py        # Step 2 - metrics computation
│   ├── detect_anomalies.py       # Step 3 - anomaly detection
│   └── aggregate_insights.py     # Step 4 - visualization & insights
│
├── notebooks/
│   └── exploration.ipynb
│
├── requirements.txt
├── README.md
└── .gitignore

