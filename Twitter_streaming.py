import tweepy 
import time
from kafka import KafkaProducer, KafkaConsumer

#twitter setup
access_token = "1371406637362159622-ICkyarNJ3SuUAc0CVZb4PgtyQFUn2R"
access_token_secret =  "UcFJuNmWXjzOyWb9GZfhFSPJZaDplqVZbgxVo45dFwtxe"
consumer_key =  "FlIKebIx45zqbsMyBeBOuPlKT"
consumer_secret =  "hW1TgjUJ7PYDZLg39mkjGWul9s8JiDskhOuh78vYXui1QpimBP"

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

producer=KafkaProducer(bootstrap_servers='localhost:9092')
topic_name='trendingnews'

def get_twitter_data():
    res=api.search("covid")
    for i in res:
        record=''
        record+=str(i)
        record+=';'
        producer.send(topic_name,str.encode(record))
        print(record)
        print("\n\n")
        producer.flush() 

get_twitter_data()

def periodic_work(interval):
    while True:
        get_twitter_data()
        time.sleep(interval)

periodic_work(6)

