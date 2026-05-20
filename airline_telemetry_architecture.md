# Airline Telemetry Streaming Architecture
## Enterprise System Design Document

---

### 1. Executive Architecture Overview

This document outlines the architecture of an **Event-Driven, End-to-End Streaming Data Pipeline** designed to track real-time airline passenger and baggage telemetry. The system is built to ingest, validate, join, and warehouse high-velocity sensor data with zero manual intervention.

**Data Flow:**
`Kafka (Local Producer) -> PySpark (Structured Streaming) -> AWS S3 (Data Lake) -> Snowflake (Snowpipe Auto-Ingest) -> Snowflake Bronze Table`

---

### 2. Core Engineering Components

#### A. The Chaos Producer (Simulation)
The origin of the pipeline is a Python-based Kafka producer that simulates real-world airport operations. It specifically generates 1-to-many telemetry events—mimicking the lifecycle of a single bag being scanned at check-in, TSA security, automated sort belts, and ramp loading. This "chaos" tests the pipeline's ability to handle duplicate identifiers and out-of-order events.

#### B. Stream Ingestion & Data Contracts
To protect downstream analytics, the ingestion layer (`msk_reader.py`) strictly enforces a **Data Contract** before any processing occurs.
* The system intercepts the Kafka JSON payload.
* It explicitly checks for nulls, empty strings (`""`), and placeholder data (e.g., `"unknown"`).
* It enforces a rigid 6-character length constraint on the Passenger Name Record (`pnr_locator`).

#### C. Stateful Stream Processing
The transformation layer (`baggage_joiner.py`) performs complex time-windowed watermarking across two live data streams (Passenger and Baggage).
* **1-to-Many Join Logic:** The system holds passenger boarding events in memory and joins them to multiple baggage scan events using an intentional time window (`baggage_time >= passenger_time - interval 3 hours AND baggage_time <= passenger_time + interval 1 hour`).
* This logic preserves the historical tracking timeline of individual bags rather than erroneously deduplicating them.

#### D. The Storage Layer (AWS S3)
Processed data is written directly to an AWS S3 Data Lake using the `s3a://` protocol.
* **Hive-Style Partitioning:** Data is chronologically partitioned (`year=YYYY/month=MM/day=DD`) to enable partition pruning, drastically reducing scan times and compute costs for downstream analytics.
* **Compression:** Data is written in columnar Parquet format using Snappy compression to optimize storage footprint.

#### E. The Serverless Ingestion (Snowflake)
The pipeline leverages a fully decoupled, event-driven architecture to load data into the cloud warehouse.
* **S3 Event Notifications:** When a new Parquet file lands in S3, AWS automatically sends a tiny JSON payload to a hidden SQS queue managed by Snowflake.
* **Snowpipe:** The SQS queue wakes up Snowflake compute, triggering a `COPY INTO` command that parses the Parquet `$1` metadata (including regex extraction of the S3 URL to recover the Hive partition columns) and loads it into the `BRONZE_B2B_JOINED` table.

---

### 3. Fault Tolerance & Dead Letter Queues (DLQ)

The architecture is designed to be self-healing and fault-tolerant, ensuring that bad data does not crash the ingestion queue.

* **Stream Splitting:** Inside PySpark, the stream is split into a "Golden Path" (valid data) and a "DLQ Path" using inverse boolean filtering (`~valid_contract`).
* **Quarantine Layer:** Failed records are tagged with an explicit `error_reason` column (e.g., `"FAILED_PNR_CONTRACT"`) and routed to a highly partitioned S3 quarantine directory (`s3://.../dlq/`).
* **Compute Isolation:** The AWS S3 Event Notification is configured with a strict prefix filter (`valid_data/`). This ensures that only Golden Data wakes up Snowflake compute, while DLQ data rests cheaply in S3 for asynchronous debugging via AWS Athena.

---

### 4. Engineering Trade-offs & Lessons Learned

During the development of this architecture, several key engineering pivots were made to adhere to enterprise best practices:

* **S3 Idempotency vs. Developer Intuition:** We discovered that Snowflake Snowpipe natively maintains a 14-day state of ingested files. Manually deleting and re-uploading a file with the identical name results in Snowpipe silently ignoring it to prevent duplicates. The solution in a streaming architecture is to always move forward, generating new files with new timestamps rather than forcing re-runs.
* **Credential Management (The FinOps Path):** We abandoned passing hardcoded AWS environment variables into PySpark. Instead, we leveraged the AWS `DefaultAWSCredentialsProviderChain`, allowing the Java SDK to seamlessly and securely inherit the user's existing local AWS CLI IAM authentication.
* **DRY Architecture (Don't Repeat Yourself):** The initial prototype featured a monolithic `main.py` script with highly redundant code. We refactored the pipeline into modular `readers`, `transformers`, and `writers`. By abstracting logic into reusable functions (e.g., `write_partitioned_stream` and `add_hive_partitions`), we created a codebase that is clean, maintainable, and ready for CI/CD pipelines.
