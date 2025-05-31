# Kafka Installation


## SQL Server Setup

```

EXEC sys.sp_cdc_enable_db;


EXEC sys.sp_cdc_enable_table  
    @source_schema = N'dbo',  
    @source_name   = N'poc_table',  
    @role_name     = NULL;

```


## Local Configuration setup

- create-connector.json

```
{
    "name": "sqlserver-connector",
    "config": {
      "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
      "database.hostname": "servername.database.windows.net",
      "database.port": "1433",
      "database.user": "username",
      "database.password": "password",
      "database.names": "database",
      "database.dbname": "database",
      "table.include.list": "dbo.tablename",
      "database.server.name": "servername",
      "database.encrypt": "true",
      "database.trustServerCertificate": "true",
      "database.connection.timeout.ms": "5000",
      "topic.prefix": "sqlserver",
      "include.schema.changes": "false",
      "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
      "schema.history.internal.kafka.topic": "schema-changes.servername"
    }
  }
  
```

` localconfig.json

```
{
    "azure_blob": {
      "connection_string": "connectionstring",
      "container_name": "containername"
    }
  }
  
```


## Docker Installation

### docker kafka setup 

```
docker-compose up -d
```

### test connection broker kafka

```
curl.exe http://localhost:8083
```



## Connector Installation
### Setup Connector Debezium SQL Server 

```
curl.exe -X POST -H "Content-Type: application/json" --data @create-connector.json http://localhost:8083/connectors
```

### Test Connection of Connector

```
curl.exe http://localhost:8083/connectors/sqlserver-connector/status
```


## Python Library Installation
```
pip install kafka-python azure-storage-blob
```


## Run Pipeline

```
python kafka_to_blob.py
```

## Kafka Topics

```
docker exec -it kafka-broker /usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic sqlserver.dbo.poc_table --from-beginning
```