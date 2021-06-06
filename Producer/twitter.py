import tweepy 
import time
import json
from kafka import KafkaProducer, KafkaConsumer

file = open("../ApiCredentials.json",)
api_keys = json.load(file)

#twitter setup
access_token = api_keys["access_token"]
access_token_secret = api_keys["access_token_secret"]
consumer_key =  api_keys["consumer_key"]
consumer_secret = api_keys["consumer_secret"]

#Creating the authentication object
auth=tweepy.OAuthHandler(consumer_key, consumer_secret)
#Setting access token and secret key
auth.set_access_token(access_token, access_token_secret)
#Creating the APU object by passing in auth information
api=tweepy.API(auth)

from datetime import datetime

def json_serializer(data):
    return json.dumps(data).encode("utf-8")  

producer=KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=json_serializer)
topic_name='twitter'
woeid=23424848
trends = api.trends_place(id = woeid)
trend_=[]
for value in trends:
    for trend in value['trends']:
        trend_.append(trend['name'])
        
def get_twitter_data():
    for trend in trend_:
        tweets = tweepy.Cursor(api.search,
                q=trend,
                tweet_mode = 'extended',
                lang="en").items(5)
        for tweet in tweets:
            record={}
            record['_id']=str(tweet.id)
            record['source'] = "twitter"
            if 'retweeted_status' in tweet._json:
                record['text']=str(tweet._json['retweeted_status']['full_text']).lower()
                record['score']=int(tweet.retweet_count)*2
                record['score']+=int(tweet._json['retweeted_status']['favorite_count'])
            else:
                record['text']=str(tweet.full_text).lower()
                record['score']=int(tweet.retweet_count)*2
                record['score']+=int(tweet.favorite_count)
            print(record)
            print('\n')
            producer.send(topic_name,record)
            producer.flush()

def periodic_work(interval):
    while True:
        get_twitter_data()
        time.sleep(interval)

periodic_work(30)
