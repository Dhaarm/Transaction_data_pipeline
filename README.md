# Overview

This project implements a data engineering pipeline that simulates transaction processing using a modern data stack. The pipeline performs data generation, ingestion into a data lake, transformation using distributed processing, and storage in a NoSQL database. The workflow is orchestrated using a scheduling system and deployed using containers.

# The pipeline demonstrates:

Data generation with realistic transaction records

Data ingestion into a Delta Lake storage layer

Data cleaning and transformation using Spark

Aggregation of customer transaction totals

Storage of results in a distributed NoSQL database

Workflow orchestration with a DAG scheduler

# Architecture

Pipeline Flow:
```
Fake Transaction Generator
        ↓
CSV Dataset
        ↓
Delta Lake (Data Lake Storage)
        ↓
Spark ETL Processing
        ↓
Customer Daily Aggregation
        ↓
ScyllaDB (NoSQL Database)
        ↓
Airflow DAG Orchestration
```
# Core technologies used:

Apache Spark

Delta Lake

Apache Airflow

ScyllaDB

Docker

# Project Structure
```data-engineer-assignment/
│
├── docker-compose.yml
│
├── data
│   └── delta
│
├── scripts
│   ├── generate_data.py
│   ├── upload_to_delta.py
│   └── spark_etl.py
│
├── airflow
│   └── dags
│       └── transaction_pipeline.py
│
└── README.md
```
# Pipeline Components
# 1 Data Generation

Script: generate_data.py

Generates 1200 fake transactions using Faker.

Fields generated:
```
transaction_id
customer_id
amount
timestamp
merchant
```
Additional characteristics:

Duplicate transactions intentionally added

Negative transaction amounts included

Random timestamps over the last 5 days

Purpose: simulate real-world dirty datasets.

# 2 Data Ingestion

Script: upload_to_delta.py

Steps:

Read generated CSV dataset

Load data into Delta Lake

Store table at:

/opt/data/delta

Delta Lake provides:

ACID transactions

Schema enforcement

Time travel capability

Efficient storage

# 3 Data Transformation (Spark ETL)

Script: spark_etl.py

Processing steps:

Remove duplicate transactions
```
dropDuplicates(transaction_id)
```
Filter invalid transactions
```
amount > 0
```
Extract transaction date
```
to_date(timestamp)
```
Aggregate daily totals

Output schema:
```
customer_id
transaction_date
daily_total
```
# 4 Load Results into ScyllaDB

Aggregated data is written into a NoSQL table.

Keyspace:
```
transactions
```
Table:
```
daily_customer_totals
```
Schema:
```
customer_id TEXT
transaction_date DATE
daily_total FLOAT
PRIMARY KEY(customer_id, transaction_date)
```
# 5 Delta Lake Time Travel

Delta Lake supports querying historical versions.

Example query:
```
spark.read.format("delta")
.option("versionAsOf", 0)
.load("/opt/data/delta")
```
This allows reading previous versions of the dataset.

# 6 Workflow Orchestration

Pipeline orchestration is handled using a DAG in Airflow.

DAG name:

transaction_pipeline

Task sequence:
```
generate_data
    ↓
upload_to_delta
    ↓
spark_etl
```
Schedule:

Daily
Setup Instructions
1 Start Containers
```
docker compose up -d
```
2 Start scheduler
```
docker exec -d airflow airflow scheduler
```
Services started:
```
Spark Master
Spark Worker
ScyllaDB
Airflow
```
Open Airflow UI:

http://localhost:8081

Trigger DAG:
```
transaction_pipeline
```

Verify Data in ScyllaDB

Connect to Scylla container:
```
docker exec -it scylladb cqlsh
```
Run queries:
```
USE transactions;

SELECT * FROM daily_customer_totals;
```
Example Output
```
customer_id | transaction_date | daily_total
---------------------------------------------
C10234      | 2026-03-15       | 452.78
C38911      | 2026-03-15       | 298.54
```
Data Quality Rules Implemented
```
| Rule                      | Description                             |
| ------------------------- | --------------------------------------- |
| Duplicate Removal         | Duplicate transaction IDs removed       |
| Negative Amount Filtering | Invalid transactions removed            |
| Date Extraction           | Timestamp converted to transaction_date |
```

Data generation with realistic anomalies

Distributed data processing

Delta Lake versioning and time travel

Aggregation of transactional data

NoSQL data storage

Workflow orchestration

Containerized deployment
