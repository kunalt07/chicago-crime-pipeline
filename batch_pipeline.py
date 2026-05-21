import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timezone

# ── CONFIG ──────────────────────────────────────────────
PROJECT_ID = "chicago-crime-pipeline"        # replace this
DATASET    = "chicago_crime_processed"
TABLE      = "batch_processed"
OUTPUT     = f"{PROJECT_ID}:{DATASET}.{TABLE}"
# ────────────────────────────────────────────────────────

# SQL query to read from public dataset
# We limit to year 2023 to keep costs low
QUERY = """
    SELECT
        unique_key,
        date,
        primary_type,
        description,
        location_description,
        arrest,
        domestic,
        district,
        year,
        latitude,
        longitude
    FROM `bigquery-public-data.chicago_crime.crime`
    WHERE year = 2023
"""

# Schema of your output BigQuery table
OUTPUT_SCHEMA = (
    "unique_key:INTEGER,"
    "date:TIMESTAMP,"
    "primary_type:STRING,"
    "description:STRING,"
    "location:STRING,"
    "arrest:BOOLEAN,"
    "domestic:BOOLEAN,"
    "district:INTEGER,"
    "year:INTEGER,"
    "latitude:FLOAT,"
    "longitude:FLOAT,"
    "processed_at:TIMESTAMP"
)


# ── TRANSFORM FUNCTION ───────────────────────────────────
# This runs on each row coming from BigQuery
def transform_row(row):
    return {
        "unique_key":   row.get("unique_key"),
        "date":         str(row.get("date")),
        "primary_type": (row.get("primary_type") or "UNKNOWN").strip().upper(),
        "description":  (row.get("description") or "UNKNOWN").strip(),
        "location":     (row.get("location_description") or "UNKNOWN").strip(),
        "arrest":       row.get("arrest", False),
        "domestic":     row.get("domestic", False),
        "district":     row.get("district"),
        "year":         row.get("year"),
        "latitude":     row.get("latitude"),
        "longitude":    row.get("longitude"),
        "processed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    }


# ── FILTER FUNCTION ──────────────────────────────────────
# Drop rows with missing critical fields
def is_valid(row):
    return (
        row.get("unique_key") is not None and
        row.get("date") is not None and
        row.get("primary_type") is not None
    )


# ── PIPELINE ─────────────────────────────────────────────
def run():
    options = PipelineOptions(
        project=PROJECT_ID,
        runner="DirectRunner",   # runs locally first — we test here before Dataflow
        temp_location=f"gs://{PROJECT_ID}-temp/temp",
        region="us-central1"
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            # Step 1: Read from BigQuery
            | "ReadFromBigQuery" >> beam.io.ReadFromBigQuery(query=QUERY, use_standard_sql=True)

            # Step 2: Filter invalid rows
            | "FilterInvalid" >> beam.Filter(is_valid)

            # Step 3: Transform each row
            | "TransformRows" >> beam.Map(transform_row)

            # Step 4: Write to your BigQuery table
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                OUTPUT,
                schema=OUTPUT_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

    print("Batch pipeline completed.")


if __name__ == "__main__":
    run()