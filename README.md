# ⚡ Real-Time Stock Data Engineering Pipeline

### 🚀 End-to-End Streaming & Data Warehouse Pipeline

![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4.1-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Transformations-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)

---

## 📌 Overview

This project implements an end-to-end data engineering pipeline for ingesting, streaming, storing, orchestrating, and processing real-time stock market data.

The pipeline retrieves stock market quotes from the **Finnhub API**, streams the data through **Apache Kafka**, stores raw JSON data in **MinIO**, orchestrates the ingestion workflow using **Apache Airflow**, and loads the data into **Snowflake** for data warehousing and further transformation using **dbt**.

The project demonstrates practical implementation of modern data engineering concepts including:

- Real-time data ingestion
- Event streaming
- Object storage
- Workflow orchestration
- Cloud data warehousing
- Data transformation
- Containerized infrastructure
- Environment-based credential management

---

## 🏗️ Architecture

```text
                    ┌─────────────────────┐
                    │    Finnhub API      │
                    │  Stock Market Data  │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Python Producer   │
                    │   API Data Fetcher  │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Apache Kafka      │
                    │   stock_quotes      │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Python Consumer   │
                    │   Kafka → MinIO     │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │       MinIO         │
                    │  Bronze Object Store│
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Apache Airflow    │
                    │ Workflow Orchestration│
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     Snowflake       │
                    │    Data Warehouse   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │        dbt          │
                    │ Transformation Layer│
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Streamlit      │
                    │ Real-Time Dashboard │
                    └─────────────────────┘
```

---

## 🚀 Quickstart Guide

### 1. Start Infrastructure
```bash
cd infra
docker compose up -d
```

### 2. Run Producer & Consumer
- **Producer** (Finnhub API ➡️ Kafka):
  ```bash
  python infra/Producer/Producer.py
  ```
- **Consumer** (Kafka ➡️ MinIO):
  ```bash
  python infra/Consumer/Consumer.py
  ```

### 3. Orchestrate with Airflow
- Open `http://localhost:8080` (Credentials: `admin` / `admin`)
- Trigger the `minio_to_snowflake` DAG to load raw data into Snowflake.

### 4. Transform Data with dbt
```bash
cd dbt_stocks
dbt run
```

### 5. Launch Real-Time Streamlit Dashboard
```bash
streamlit run dashboard/app.py
```
Or simply run `run_dashboard.bat` from the root folder!
Access the dashboard at: `http://localhost:8501`