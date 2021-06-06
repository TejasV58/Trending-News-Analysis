
import requests
from bs4 import BeautifulSoup
import json
import time
from kafka import KafkaProducer
from newsapi.newsapi_client import NewsApiClient

file = open("../ApiCredentials.json",)
api_keys = json.load(file)

topic_name='headlines'

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

news_headlines = []

producer=KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=json_serializer)

def get_inshorts_news(category):
    newsDictionary = []
    try:
        htmlBody = requests.get('https://www.inshorts.com/en/read/' + category)
    except requests.exceptions.RequestException as e:
        newsDictionary['success'] = False
        newsDictionary['errorMessage'] = str(e.message)
        return newsDictionary

    soup = BeautifulSoup(htmlBody.text, 'lxml')
    newsCards = soup.find_all(class_='news-card')
    if not newsCards:
        newsDictionary['success'] = False
        newsDictionary['errorMessage'] = 'Invalid Category'
        return newsDictionary
    for card in newsCards:
        try:
            title = card.find(class_='news-card-title').find('a').text
        except AttributeError:
            title = None
        try:
            content = card.find(class_='news-card-content').find('div').text
        except AttributeError:
            content = None
          
        try:
            date = card.find(class_='date').text
        except AttributeError:
            date = None

        try:
            time = card.find(class_='time').text
        except AttributeError:
            time = None

        newsObject = {
            '_id':time+"-"+date,
            'text': title,
            'source':"inshorts"
        }
        news_headlines.append(newsObject)
        print(newsObject)
        print('\n')
        # producer.send(topic_name,newsObject)
        # producer.flush()
       

def store_inshorts_news():
    categories=["national","business","sports","world","politics","technology","startup","entertainment","miscellaneous","hatke","science","automobile"]
    for category in categories:
        get_inshorts_news(category)

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
            news_headlines.append(newsObject)
            print(newsObject)
            print('\n')
            # producer.send(topic_name,newsObject)
            # producer.flush()

url = "https://contextualwebsearch-websearch-v1.p.rapidapi.com/api/search/TrendingNewsAPI"

querystring = {"pageNumber":"1","pageSize":"50","withThumbnails":"false","location":"in"}

headers = {
    'x-rapidapi-key': "fc7294a7bfmsh4671f20cbd570f7p12e74ejsn9e1650af22e1",
    'x-rapidapi-host': "contextualwebsearch-websearch-v1.p.rapidapi.com"
    }

def get_websearch_news():
    responses = requests.request("GET", url, headers=headers, params=querystring).json()
    for response in responses["value"]:
        newsObject = {
            '_id':response["id"],
            'text': response["title"],
            'source':"websearch"
        }
        news_headlines.append(newsObject)
        print(newsObject)
        print('\n')
        # producer.send(topic_name,newsObject)
        # producer.flush()

while True:
    get_websearch_news()
    store_inshorts_news()
    get_newsapi_news()
    for headline in news_headlines:
        producer.send(topic_name,headline)
        producer.flush()
    print(len(news_headlines))
    news_headlines = []
    time.sleep(1800)
