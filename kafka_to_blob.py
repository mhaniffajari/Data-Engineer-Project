from kafka import KafkaConsumer
from azure.storage.blob import BlobServiceClient
import json
import uuid

# Load config from localconfig.json
with open("localconfig.json") as config_file:
    config = json.load(config_file)
    connection_string = config["azure_blob"]["connection_string"]
    container_name = config["azure_blob"]["container_name"]

# Kafka settings
TOPIC_NAME = "sqlserver.poc_database.dbo.poc_table"
KAFKA_BROKER = "localhost:29092"

# Initialize Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Set up Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='blob-writer-group'
)

print(f"Listening to topic: {TOPIC_NAME}")
for message in consumer:
    data = message.value
    blob_name = f"event-{uuid.uuid4()}.json"
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json.dumps(data), overwrite=True)
    print(f"Uploaded to Blob: {blob_name}")
