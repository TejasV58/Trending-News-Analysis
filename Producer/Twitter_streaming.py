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

def normalize_timestamp(time):
    mytime=datetime.strptime(time,"%Y-%m-%d %H:%M:%S")
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))

def json_serializer(data):
    return json.dumps(data).encode("utf-8")  

producer=KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=json_serializer)
topic_name='news'

def get_twitter_data():
    tweets = tweepy.Cursor(api.search,
              q=topic_name,
              tweet_mode = 'extended',
              lang="en").items(1)
    for tweet in tweets:
        record={}
        record['text']=str(tweet.full_text) 
        record['retweet_count'] = tweet.retweet_count
        print(record)
        producer.send(topic_name,record)
        producer.flush()
    # res=api.search("news")
    # for i in res:
    #     json_str = json.dumps(i._json)
    #     parsed = json.loads(json_str)
    #     record=''
    #     record+=str(i.text +"\t"+ str(i.retweet_count))
    #     record+=';'
    #     print(record)
    #     producer.send(topic_name,str.encode(record))
    #     producer.flush() 

get_twitter_data()

def periodic_work(interval):
    while True:
        get_twitter_data()
        time.sleep(interval)

periodic_work(5)
