# Chicago Crime Data Pipeline — GCP

End-to-end data engineering project built on Google Cloud Platform, processing 
Chicago Police Department crime records using both batch and real-time streaming 
pipelines.

## Architecture Overview

Two parallel pipelines handle historical and real-time data:

- **Batch Pipeline** — Reads 1M+ records from BigQuery public dataset, applies 
data quality checks and transformations using Apache Beam, and loads clean data 
into a partitioned BigQuery table
- **Streaming Pipeline** — Simulates real-time crime event ingestion via Pub/Sub, 
processes messages through a Beam streaming pipeline, and writes to a separate 
BigQuery table with ingestion timestamps
- **Orchestration** — Cloud Composer (Airflow) DAG schedules the batch pipeline 
daily, managing dependencies and retry logic

## Tech Stack

| Tool | Purpose |
|---|---|
| BigQuery | Data warehouse — source and destination |
| Apache Beam (Python) | Batch and streaming transformations |
| Google Dataflow | Managed distributed Beam runner |
| Pub/Sub | Real-time message ingestion layer |
| Cloud Composer (Airflow) | Pipeline orchestration and scheduling |
| Cloud Storage | Temp storage for Dataflow jobs |

## Pipeline Flow
Chicago Crime Public Dataset (BigQuery Public Data)
↓
[BATCH PIPELINE]
Apache Beam on Dataflow

Filter invalid records
Standardize crime type fields
Add processed_at metadata timestamp
↓
BigQuery — batch_processed table (2023 crime records)

[STREAMING PIPELINE]
Publisher Script (Python)
↓
Pub/Sub Topic — chicago-crime-topic
↓
Apache Beam Streaming Pipeline on Dataflow

Parse JSON messages
Add ingested_at timestamp
↓
BigQuery — streaming_processed table

[ORCHESTRATION]
Cloud Composer (Airflow)

DAG: chicago_crime_batch_pipeline
Schedule: @daily
Retry logic: 1 retry with 5-minute delay


## Data Quality Steps

- Dropped records missing `unique_key`, `date`, or `primary_type`
- Standardized `primary_type` field to uppercase
- Replaced null location fields with `UNKNOWN`
- Added pipeline metadata timestamps (`processed_at`, `ingested_at`)

## Results

| Pipeline | Records Processed | Destination |
|---|---|---|
| Batch | 1M+ (2023 data) | `batch_processed` |
| Streaming | 50 simulated events | `streaming_processed` |

## Key Learnings

- Apache Beam's `PCollection` model vs PySpark DataFrame approach
- Difference between batch and streaming pipeline configurations in Dataflow
- Pub/Sub message serialization and deserialization
- Cloud Composer DAG structure and scheduling