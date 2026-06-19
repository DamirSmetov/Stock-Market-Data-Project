# 🚀 Stock Market Data Pipeline

An end-to-end **data engineering system** for real-time and batch processing of stock market data using distributed systems, streaming architecture, and cloud data warehousing.

---

# 📌 Project Overview

This project demonstrates how real-world data engineering systems are built to handle both streaming and batch workloads.

It processes live financial data from Yahoo Finance and Alpha Vantage, streams it through Kafka, transforms it using Spark, stores intermediate data in MinIO (S3-compatible storage), orchestrates workflows with Airflow, and loads curated datasets into Snowflake for analytics and reporting.

---

# 🏗️ Architecture

![Stock Pipeline Architecture](/img/Stock%20Market%20Data%20Architecture.png)

---

# 🧱 Data Lake Architecture (Medallion Model)

- 🟤 Bronze Layer → Raw Kafka data stored in MinIO
- ⚪ Silver Layer → Cleaned Spark transformations
- 🟡 Gold Layer → Aggregated analytics in Snowflake

---

# ⚙️ Tech Stack

| Technology      | Purpose                      |
| --------------- | ---------------------------- |
| Kafka           | Real-time message streaming  |
| Spark           | Batch + streaming processing |
| Spark Streaming | Real-time data processing    |
| MinIO           | S3-compatible storage        |
| Snowflake       | Cloud data warehouse         |
| Airflow         | Workflow orchestration       |
| PostgreSQL      | Metadata storage             |
| Docker          | Containerization             |
| Python          | Core logic                   |

---

# 🔄 Pipeline Workflow

## 1. Data Ingestion

- Stock data from Yahoo Finance & Alpha Vantage
- Sent into Kafka topics

## 2. Stream Processing

- Spark Structured Streaming processes real-time data
- Cleans and transforms records

## 3. Batch Processing

- Spark batch jobs process historical data

## 4. Storage Layer

- MinIO stores:
  - CSV (raw)
  - Parquet (processed)

## 5. Data Warehouse

- Snowflake stores curated datasets for analytics

## 6. Orchestration

- Airflow manages:
  - ingestion
  - processing
  - loading pipelines
- Includes retries and dependencies

## 7. Analytics

- Qlik Sense dashboards:
  - stock performance tracking
  - trend analysis
  - market insights

---

# 🔁 Reliability & Fault Tolerance

- Kafka ensures replayable event streams
- Spark checkpointing for fault recovery
- Airflow retries failed tasks
- Idempotent pipeline design

---

# 📊 Data Quality

Before loading into Snowflake:

- Schema validation
- Null checks
- Duplicate removal
- Basic anomaly filtering

---

# ⚡ Spark Optimizations

- Partitioned processing
- Parallel execution
- Reduced shuffle operations
- Streaming checkpoints

---

# 📡 Streaming Design

- Kafka topics per financial instrument
- Low-latency real-time ingestion
- Backpressure handling in Spark

---

# 📦 Scalability

- Kafka partitions scale ingestion
- Spark workers scale compute
- MinIO scales storage
- Independent batch & streaming scaling

---

# 🐳 Infrastructure

All services run in Docker:

- Kafka + ZooKeeper
- Spark Master + Workers
- Airflow (Webserver + Scheduler)
- PostgreSQL
- MinIO

---

# 📊 Scale

- ~28,800 records/day
- 20 financial instruments
- Hybrid batch + streaming system

---

# 🧠 Skills Demonstrated

- Distributed systems (Kafka, Spark)
- Streaming pipelines
- Data warehousing (Snowflake)
- Orchestration (Airflow)
- ETL design
- Cloud storage systems
- Medallion architecture
- System reliability design

---

# 🚀 Future Improvements

- Monitoring (Prometheus + Grafana)
- Kubernetes deployment
- CI/CD pipeline (GitHub Actions)
- Data quality framework (Great Expectations)
- Real-time alerting system

---

# 👤 Author

Data Engineering portfolio project demonstrating real-world streaming + batch architecture.
