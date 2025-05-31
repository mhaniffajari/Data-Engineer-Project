#!/bin/sh

curl -X "POST" "http://kafka-connect-cp:18083/connectors/" \
     -H "Content-Type: application/json" \
    -d '{
  "name": "s3-sink-ratings",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "key.converter":"org.apache.kafka.connect.storage.StringConverter",
    "tasks.max": "1",
    "topics": "ratings-with-customer-data",
    "s3.region": "us-west-2",
    "s3.bucket.name": "rmoff-demo-ratings",
    "s3.part.size": "5242880",
    "flush.size": "3",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
    "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
    "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
    "schema.compatibility": "NONE"
    }
}'