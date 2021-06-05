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

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=json_serializer)

def get_newsapi_news():
    api = api_keys["newsapikey"]
    newsapi = NewsApiClient(api_key=api)
    news = newsapi.get_top_headlines(country='in',page_size=100,language='en')
    if news['articles']!=[]:
        for article in news['articles']:
            newsObject = {
                '_id':article["publishedAt"],
                'text': article["title"],
                'source':"newsapi"
            }
            print(newsObject)
            print('\n')
            producer.send(topic_name,newsObject)
            producer.flush()

    
def store_newsapi_news():
    get_newsapi_news()

store_newsapi_news()
