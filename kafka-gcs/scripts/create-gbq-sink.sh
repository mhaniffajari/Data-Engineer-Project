#!/bin/sh
# Project and dataset must exist
# Create dataset through web UI or with CLI: 
#   ./google-cloud-sdk/bin/bq mk ksql_demo
#
curl -X "POST" "http://kafka-connect-cp:18083/connectors/" \
     -H "Content-Type: application/json" \
    -d '{
  "name": "gbq-sink-ratings",
  "config": {
    "key.converter":"org.apache.kafka.connect.storage.StringConverter",
    "topics": "ratings-with-customer-data",
    "connector.class":"com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
    "tasks.max":"1",
    "sanitizeTopics":"true",
    "autoCreateTables":"true",
    "autoUpdateSchemas":"true",
    "schemaRetriever":"com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever.SchemaRegistrySchemaRetriever",
    "schemaRegistryLocation":"http://schema-registry:8081",
    "bufferSize":"100000",
    "maxWriteSize":"10000",
    "tableWriteWait":"1000",
    "project":"devx-testing",
    "datasets":".*=ksql_demo",
    "keyfile":"/root/gcp_creds.json"
    }
}'