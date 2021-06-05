# from inshorts import store_inshorts_news
# from newsapi_producer import store_websearch_news
# from websearch import store_newsapi_news
import time

# store_newsapi_news(1800)
# store_websearch_news(1800)
# store_inshorts_news(1800)

import os

while True:
    os.system('python3 ./newsapi_producer.py')
    os.system('python3 ./websearch.py')
    os.system('python3 ./inshorts.py')
    time.sleep(1800)

