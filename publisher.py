import json
import time
from google.cloud import bigquery, pubsub_v1

# ── CONFIG ──────────────────────────────────────────────
PROJECT_ID = "chicago-crime-pipeline"       # replace this
TOPIC_ID   = "chicago-crime-topic"
# ────────────────────────────────────────────────────────

def publish_messages():
    bq_client  = bigquery.Client(project=PROJECT_ID)
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    # Read sample rows from your batch table
    query = """
        SELECT unique_key, date, primary_type, arrest
        FROM `chicago_crime_processed.batch_processed`
        LIMIT 50
    """

    rows = bq_client.query(query).result()

    print("Publishing messages to Pub/Sub...")

    for row in rows:
        message = {
            "unique_key":   row["unique_key"],
            "date":         str(row["date"]),
            "primary_type": row["primary_type"],
            "arrest":       row["arrest"]
        }

        # Pub/Sub requires bytes
        data = json.dumps(message).encode("utf-8")
        publisher.publish(topic_path, data=data)

        print(f"Published: {message['primary_type']} - {message['date']}")
        time.sleep(0.5)   # simulate real-time delay

    print("Done. 50 messages published.")

if __name__ == "__main__":
    publish_messages()