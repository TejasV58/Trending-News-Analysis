import requests
from bs4 import BeautifulSoup
import datetime


def getNews(category):
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
            date = card.find(clas='date').text
        except AttributeError:
            date = None

        try:
            time = card.find(class_='time').text
        except AttributeError:
            time = None
  
        newsObject = {
            'title': title
        }
        print(newsObject)
        print('\n')

categories=["national","business","sports","world","politics","technology","startup","entertainment","miscellaneous","hatke","science","automobile"]
for category in categories:
    news=getNews(category)
    print(news)