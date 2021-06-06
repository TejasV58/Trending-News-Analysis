import requests
import json
import time
from kafka import KafkaProducer, KafkaConsumer

url = "https://contextualwebsearch-websearch-v1.p.rapidapi.com/api/search/TrendingNewsAPI"

querystring = {"pageNumber":"1","pageSize":"100","withThumbnails":"false","location":"in"}

headers = {
    'x-rapidapi-key': "fc7294a7bfmsh4671f20cbd570f7p12e74ejsn9e1650af22e1",
    'x-rapidapi-host': "contextualwebsearch-websearch-v1.p.rapidapi.com"
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer=KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=json_serializer)

topic_name='headlines'

def get_websearch_news():
    responses = requests.request("GET", url, headers=headers, params=querystring).json()
    for response in responses["value"]:
        newsObject = {
            '_id':response["id"],
            'text': response["title"],
            'source':"websearch"
        }
        print(newsObject)
        print('\n')
        producer.send(topic_name,newsObject)
        producer.flush()

def store_websearch_news():
    get_websearch_news()

store_websearch_news()