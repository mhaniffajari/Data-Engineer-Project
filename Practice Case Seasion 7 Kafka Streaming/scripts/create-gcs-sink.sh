#!/bin/sh

curl -X "POST" "http://kafka-connect-cp:18083/connectors/" \
     -H "Content-Type: application/json" \
    -d '{
  "name": "gcs-sink-ratings",
  "config": {
    "connector.class": "io.confluent.connect.gcs.GcsSinkConnector",
    "key.converter":"org.apache.kafka.connect.storage.StringConverter",
    "tasks.max": "1",
    "topics": "ratings-with-customer-data",
    "gcs.bucket.name": "rmoff-demo-ratings",
    "gcs.part.size": "5242880",
    "flush.size": "64",
    "gcs.credentials.path": "/root/gcp_creds.json",
    "storage.class": "io.confluent.connect.gcs.storage.GcsStorage",
    "format.class": "io.confluent.connect.gcs.format.json.JsonFormat",
    "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
    "confluent.topic.bootstrap.servers": "kafka:29092",
    "confluent.topic.replication.factor"
    }
}'