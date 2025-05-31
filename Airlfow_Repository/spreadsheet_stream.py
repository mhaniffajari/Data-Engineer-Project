from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
def send_record():
    producer_config = {'bootstrap.servers': 'kafka1.dev.bri.co.id:9093,kafka2.dev.bri.co.id:9093,kafka3.dev.bri.co.id:9093',
            'schema.registry.url': 'https://kafka12.dev.bri.co.id:8181',
            #'schema.registry.ssl.ca.location':'/var/ssl/private/control_center.crt',
            #'schema.registry.ssl.ca.location':'mycert.pem',
            #'schema.registry.ssl.key.location':'/var/ssl/private/control_center.key',
            #'schema.registry.ssl.certificate.location':'/var/ssl/private/control_center.crt',
            #'ssl.ca.location':'/var/ssl/private/ca.crt',
            #'ssl.key.location':'/var/ssl/private/control_center.key',
            #'ssl.certificate.location':'/var/ssl/private/control_center.crt',
            'security.protocol':'SASL_SSL',
            'sasl.username':'BRIKAFKAEDWCLDSVC',
            'sasl.password':'Pwd&L7GCtJ',
            'sasl.mechanism':'PLAIN',
            'schema.registry.basic.auth.credentials.source':'USER_INFO',
            'schema.registry.basic.auth.user.info':'micro_cdc:Pwd&M1Cro'}

    producer = AvroProducer(producer_config)
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    gc = gspread.authorize(credentials)
    spreadsheet_key = '1zinHUl9YA3i4He3p0POumZ6Fez1oGqzI4ET1CaE_NZY'
    book = gc.open_by_key(spreadsheet_key)
    worksheet = book.worksheet("Purchase_Dataset")
    table = worksheet.get_all_values()
    value=table
    try:
        producer.produce(topic='edw_test_123', value=value)
    except Exception as e:
        print(f"Exception while producing record value - {value}: {e}")
    else:
        print(f"Successfully producing record value - {value}")

        producer.flush()
        
if __name__ == "__main__":
    send_record()