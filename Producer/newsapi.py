import json
import time
import datetime
from pytz import timezone    
from kafka import KafkaProducer
from newsapi.newsapi_client import NewsApiClient

file = open("../ApiCredentials.json",)
api_keys = json.load(file)

def json_serializer(data):
    return json.dumps(data).encode("utf-8") 

topic_name='headlines'

def kafka_producer_news(producer):
    api = api_keys["newsapikey"]
    newsapi = NewsApiClient(api_key=api)
    news = newsapi.get_top_headlines(country='in',page_size=70,language='en')
    if news['articles']!=[]:
        for article in news['articles']:
            newsObject = {
                'title': article["title"]
            }
            producer.send(topic_name,newsObject)


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=json_serializer)
    kafka_producer_news(producer)
    
