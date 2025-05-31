#!/bin/sh
# This lets you write data that KSQL has output as CSV (DELIMITED)
# to GCS without trying to convert it to Json etc-useful for Data Studio

curl -X "POST" "http://kafka-connect-cp:18083/connectors/" \
     -H "Content-Type: application/json" \
    -d '{
  "name": "gcs-sink-ratings-csv",
  "config": {
    "connector.class": "io.confluent.connect.gcs.GcsSinkConnector",
    "key.converter":"org.apache.kafka.connect.storage.StringConverter",
    "value.converter":"org.apache.kafka.connect.converters.ByteArrayConverter",
    "tasks.max": "1",
    "topics": "ratings-with-customer-data-csv",
    "gcs.bucket.name": "rmoff-demo-ratings-csv-time",
    "gcs.part.size": "5242880",
    "flush.size": "2048",
    "gcs.credentials.path": "/root/gcp_creds.json",
    "storage.class": "io.confluent.connect.gcs.storage.GcsStorage",
    "format.class": "io.confluent.connect.gcs.format.bytearray.ByteArrayFormat",
    "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
    "confluent.topic.bootstrap.servers": "kafka:29092",
    "confluent.topic.replication.factor":1    }
}'