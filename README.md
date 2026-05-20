# ✈️ Airline Telemetry Medallion Pipeline

An end-to-end, event-driven data engineering pipeline designed to ingest, process, and analyze high-velocity airline telemetry data. This project tracks baggage scans and passenger boarding events to calculate real-time operational efficiency and flag mishandled luggage.

## 🏗️ Architecture Overview

This project implements a modern **Medallion Architecture** (Bronze, Silver, Gold) utilizing a hybrid tech stack of containerized streaming, cloud data warehousing, and automated orchestration. 

### Tech Stack
* **Streaming Ingestion:** Apache Kafka (Docker/WSL2)
* **Stream Processing:** Apache Spark (PySpark Structured Streaming)
* **Cloud Storage & DLQ:** AWS S3
* **Cloud Data Warehouse:** Snowflake
* **Serverless Load:** Snowflake Snowpipe
* **Data Transformation:** dbt Core (Data Build Tool)
* **Orchestration:** Apache Airflow (Docker Compose)
* **Infrastructure Management:** Python (Metadata-driven deployment)

## 🔀 Data Flow Diagram

```text
[Kafka Topics: Baggage/Passenger]
          │
          ▼
[PySpark Streaming Job] ──(Bad Data)──> [AWS S3 Dead Letter Queue (DLQ)]
          │
     (Valid Joins)
          │
          ▼
[AWS S3 Bronze Stage] ──(SQS Event Notification)──> [Snowflake Snowpipe]
                                                          │
                                                          ▼
                                            [Snowflake Bronze Layer (Raw)]
                                                          │
                                     (Orchestrated by Apache Airflow & dbt)
                                                          │
                                                          ▼
                                            [Snowflake Silver Layer (Cleansed)]
                                                          │
                                                          ▼
                                            [Snowflake Gold Layer (Aggregated KPIs)]
```

## 📂 Repository Structure

```text
airport-project/
├── .env                        # Environment variables (Ignored in Git)
├── README.md                   # Project documentation
├── deploy_infrastructure.py    # Python script for idempotent Snowflake deployment
├── infrastructure/
│   └── snowflake/              # Raw SQL migration scripts (Foundation, Stage, Tables, Pipes)
├── airline_transformation/     # dbt Core project directory
│   ├── dbt_project.yml
│   ├── profiles.yml            # Dynamic credential loading
│   └── models/
│       ├── src_airline.yml     # Bronze source definitions
│       ├── silver/             # Cleansed models & data quality tests
│       └── gold/               # Aggregated business KPIs & rule tests
└── orchestration/              # Apache Airflow directory
    ├── Dockerfile              # Custom Airflow image with dbt-snowflake adapter
    ├── docker-compose.yaml     # Multi-container cluster definition
    └── dags/
        └── airline_telemetry_dag.py # Pipeline orchestration logic
```

## 🚀 Setup & Execution Guide

Follow these exact steps to spin up the entire pipeline on your local machine.

### Prerequisites
* **Docker Desktop** (configured with WSL2 on Windows)
* **Python 3.10+**
* **AWS Account** (S3 Bucket & IAM Role configured)
* **Snowflake Account**

### Step 1: Environment Configuration
Create a `.env` file in the root directory and populate it with your specific cloud credentials:
```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_locator
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### Step 2: Deploy Snowflake Infrastructure
This project uses a metadata-driven Python script to idempotently deploy the Snowflake environment (Databases, Warehouses, S3 Storage Integrations, Tables, and Snowpipe).

Activate your Python virtual environment and run:
```bash
python deploy_infrastructure.py
```
*Note: If this is your first time deploying the Storage Integration, you must manually update your AWS IAM Role Trust Policy with the newly generated `STORAGE_AWS_EXTERNAL_ID` found in the script logs.*

### Step 3: Spin up Airflow Orchestration
Navigate to the `orchestration` directory. We will build a custom Docker image that includes the `dbt-snowflake` adapter and initialize the Airflow Postgres backend.

```bash
cd orchestration

# 1. Initialize the Airflow Database and User
docker-compose up airflow-init

# 2. Start the Airflow Cluster in the background (Wait for exit code 0 on init first)
docker-compose up -d
```

### Step 4: Trigger the Pipeline
1. Open your browser and navigate to the Airflow UI at `http://127.0.0.1:8080`.
2. Log in using the default credentials (`Username: airflow` / `Password: airflow`).
3. Locate the `airline_telemetry_medallion_pipeline` DAG.
4. Toggle the DAG to **Unpaused** (Blue).
5. Click the **Play (▶)** button and select **Trigger DAG**.
6. Navigate to the **Grid View** to monitor the real-time execution of the dbt transformation models and data quality test assertions.

---
*Note: This architecture is currently managed via Python scripts. A future release (V2) will migrate infrastructure provisioning to Terraform and implement GitHub Actions for CI/CD.*