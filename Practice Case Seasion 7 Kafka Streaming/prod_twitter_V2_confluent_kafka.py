import tweepy
from confluent_kafka import Producer
import logging

"""API ACCESS KEYS"""

consumerKey = "ekPCerrEMOQcWBq7V5pRQo7Ki"
consumerSecret = "CRBrvWdhWLLNrla0sU15xNN6X8wMYVgwddTi8ymrdbRuHEdoUw"
accessToken = "231272451-Sgwq9mPDlKfXZ4NHitOUUCo54uX7YSYRn5YrfYxg"
accessTokenSecret = "zG1PNoxW36uUSDmL2fUaNG2NIClio0DYV10AeCy5TqtZR"
bearer_token = "AAAAAAAAAAAAAAAAAAAAABEihwEAAAAABPdO63dsnwN8P%2BMMt8xwzPpjKDs%3DaodLyVAdMwYKZKtMEchgENHjRtoDN3loDGf4EJS4rY8Fa7k1As"


producer = Producer({'bootstrap.servers':'localhost:9092'})
tweet_fields = ["id","text","created_at"]
topic_name = 'twitter'

def twitterAuth():
    # create the authentication object
    authenticate = tweepy.OAuthHandler(consumerKey, consumerSecret)
    # set the access token and the access token secret
    authenticate.set_access_token(accessToken, accessTokenSecret)
    # create the API object
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    return api

class TweetListener(tweepy.StreamingClient):

    def on_data(self, raw_data):
        logging.info(raw_data)
        producer.produce(topic_name, value=raw_data)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    def start_streaming_tweets(self, tweet_fields):
        self.filter(tweet_fields=tweet_fields)

if __name__ == '__main__':
    twitter_stream = TweetListener(bearer_token)
    twitter_stream.add_rules(tweepy.StreamRule("Jokowi"))
    twitter_stream.start_streaming_tweets(tweet_fields)