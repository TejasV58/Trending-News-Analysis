import json
import time
import datetime
from pytz import timezone    
from kafka import KafkaProducer
from newsapi.newsapi_client import NewsApiClient

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def get_news():
    api='61f71f614068480c9da74663df3f2ca5'
    newsapi = NewsApiClient(api_key=api)
    top_headlines = newsapi.get_top_headlines(country='in',page_size=70,language='en')
    return top_headlines    

def kafka_producer_news(producer):
    news=get_news()
    if news['articles']!=[]:
        for article in news['articles']:
            now_timezone=datetime.datetime.now(timezone(timeZone))
            producer.send(topic='news', value=bytes(str(article), 'utf-8'))
            #print("Sent economy news : {}".format(now_timezone))


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=json_serializer,api_version = (0,10))
news=get_news()

if __name__ == "__main__":
    for article in news['articles']:
        print(article)
        producer.send("newsapi",article)
        time.sleep(4)

