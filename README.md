# Data-Project

## Research on High-Performance Vectorized Execution Engine in DuckDB

### Project Overview

This project explores the performance boundaries of modern analytical databases in a single-machine environment. By leveraging DuckDB’s vectorized execution engine, we built a high-performance interactive dashboard capable of performing sub-second OLAP queries on millions of NYC Taxi trip records.

### Key Features

Sub-second Aggregations: Real-time filtering and grouping on 2.9M+ records using DuckDB.

Internals Visualization: Built-in module to inspect physical query plans (e.g., SEQ_SCAN, HASH_GROUP_BY).

Benchmarking Suite: A comparative analysis demonstrating a 93x speedup over traditional row-based systems like MySQL.

Zero-Copy Integration: Direct querying of Parquet files with zero serialization overhead.

### Technical Stack

Database: DuckDB 1.5.0 (Vectorized Execution, Columnar Storage)

Frontend: Streamlit 1.55.0

Language: Python 3.11

Visuals: Plotly Express

### CRITICAL: Database Connection & Usage Note

Please read carefully before running the code:

The current implementation uses a persistent DuckDB database file (taxi_data.db). Since the application does not utilize a dedicated "Read-Only" mode for concurrent access, DuckDB enforces a file lock to ensure data integrity.

To avoid Connection Error or Database Locked exceptions, please follow these steps:

  1. Run only one application at a time.

  2. If you are running the Streamlit dashboard (app_final.py), completely shut down the process (Ctrl+C in terminal) before opening the Jupyter Benchmark notebook.

  3. Conversely, close the Jupyter kernel before restarting the Streamlit dashboard.

### Project Structure

app_final.py: The main interactive dashboard application.

Experiment_and_Benchmark.ipynb: Notebook containing performance comparison tests (MySQL vs. DuckDB).

taxi_zone_lookup.csv: Metadata for NYC borough mapping.

yellow_tripdata_2024-01.parquet: The primary dataset (Download link provided below).
