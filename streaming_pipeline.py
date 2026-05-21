import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime, timezone

# ── CONFIG ──────────────────────────────────────────────
PROJECT_ID    = "chicago-crime-pipeline"       # replace this
TOPIC         = f"projects/{PROJECT_ID}/topics/chicago-crime-topic"
OUTPUT_TABLE  = f"{PROJECT_ID}:chicago_crime_processed.streaming_processed"
# ────────────────────────────────────────────────────────

OUTPUT_SCHEMA = (
    "unique_key:INTEGER,"
    "date:TIMESTAMP,"
    "primary_type:STRING,"
    "arrest:BOOLEAN,"
    "ingested_at:TIMESTAMP"
)

def parse_message(message):
    data = json.loads(message.decode("utf-8"))
    data["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return data

def run():
    options = PipelineOptions(
        project=PROJECT_ID,
        runner="DirectRunner",
        temp_location=f"gs://chicago-crime-pipeline-temp/temp",
        region="us-central1",
        streaming=True
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            # Step 1: Read from Pub/Sub
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=TOPIC)

            # Step 2: Parse JSON message
            | "ParseMessage" >> beam.Map(parse_message)

            # Step 3: Write to BigQuery
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE,
                schema=OUTPUT_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run()