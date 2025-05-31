# def keys
bearer_token = r'your bearer token here'
api_key = 'your consumer api key here'
api_secret = 'your consumer api secret here'
access_token = 'your access token here'
access_secret = 'your access secret here'

# import dependencies
from confluent_kafka import Producer
from faker import Faker
import json
import time
import logging
import random
import tweepy

# def tweepy
client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_secret)
auth = tweepy.OAuth1UserHandler (api_key, api_secret, access_token, access_secret)
api = tweepy.API(auth)
search = ['kanjuruhan', 'valorant']

# def logging
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# def producer
p=Producer({'bootstrap.servers':'localhost:9092'})
print('Kafka Producer has been initiated...')

# def receipt
def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)
        
# def stream tweet
class MyStream(tweepy.StreamingClient):
    def on_connect(self):
        print("Connected")


    def on_tweet(self, tweet):
        if tweet.tweets == None:
            p.poll(1)
            p.produce('twt_streaming', tweet.text, callback=receipt)
            p.flush()
            time.sleep(1)
        return

# def instance
stream = MyStream(bearer_token= bearer_token)
stream.add_rules(tweepy.StreamRule(search))
stream.filter(tweet_fields=["tweets"])
