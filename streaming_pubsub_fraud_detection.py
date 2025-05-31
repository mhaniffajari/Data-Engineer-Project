import os
os.environ['CONDA_DLL_SEARCH_MODIFICATION_ENABLE'] = '1'

# from confluent_kafka import avro
# from confluent_kafka.avro import AvroProducer

import csv
from time import sleep
import os
from google.cloud import pubsub_v1
import json
import base64
credentials_path = 'google_credentials.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/clever-seat-363006/topics/fraud_dataset_timestamp_stream'


def send_record():
     file = open('fraud_dataset_timestamp.csv')
     csvreader = csv.reader(file)
     for row in csvreader:
         attributes = {"type": str(row[0]), "amount": str(row[1]), "nameOrig": str(row[2]), "oldbalanceOrg": str(row[3]),"newbalanceOrig":str(row[4]),"nameDest":str(row[5]), "oldbalanceDest": str(row[6]), "newbalanceDest": str(row[7]),"isFraud":str(row[8]), "isFlaggedFraud":str(row[9]),"timestamp":str(row[10])}
         try:
             attributes_dumped = json.dumps(attributes)
             future = publisher.publish(topic_path, attributes_dumped.encode("utf-8"))
         except Exception as e:
             print(f"Exception while producing record value - {attributes}: {e}")
         else:
             print(f"Successfully producing record value - {attributes}")

         print(f'published message id {future.result()}')
         sleep(1)

if __name__ == "__main__":
     send_record()