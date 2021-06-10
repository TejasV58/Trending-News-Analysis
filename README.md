# Trending-News-Analysis

## Introduction

News is all around us but sometimes we might miss some important news or event occurring at a place because of the vastness of it on the internet. There are various online sources of news: social media platforms like Twitter, Instagram, Facebook, etc news websites  & applications like ndtv, India today, inshorts, etc. It would be better if the news from these various sources is fetched, processed, and only the most commonly occurring in all these sources is made available to a newsreader. To achieve this 3 step process, i.e real-time collection of news data from various sources, integrating it, processing it, and finally storing it in a database needs a technology known as Data Pipelining. 

To achieve this task we have used Kafka, spark and mongodb database. We are fetching the news headlines from newsapi,websearch API and also from inshorts. We use Kafka to act as mediator for storing temporarily before we apply any processing on data. The data stored is then fetched using Apache Spark for processing. Finally the results are stored in a MongoDB database.

## Requirements

### Kafka installed ( version =  2.13-2.70)
- kafka (python api)
- kafka-python 

### Apache Spark installed (version = version 3.1.0)
- findspark
- pyspark
- spark nlp

### newsapi
- newsapi-python

### inshorts
- beautifulsoup4
- lxml

### Twitter developer account to get API keys and access tokens

### API Keys for Web Search API

### MongoDB account, to make a cluster with database and collection

## Steps to Run-

1.Making a cluster in mongodb 

2.Creating the Database TrendingNewsDatabase and collection Headlines 

3.Start Kafka zookeeper and kafka server using the above commands:

4.Start zookeeper using :
```console
bin/zookeeper-server-start.sh config/zookeeper.properties
```
5.Start Kafka using :
```console
JMX_PORT=8004 bin/kafka-server-start.sh config/server.properties
```
6.Run headlines_streaming.py using the command below:
```console
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,com.johnsnowlabs.nlp:spark-nlp_2.12:3.0.3,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 headlines_streaming.py
```
7.Run producer.py in Producer directory on terminal:
```console
python3 producer.py
``` 
8.Running structured_streaming.py using the commands below:
```console
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,com.johnsnowlabs.nlp:spark-nlp_2.12:3.0.3,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 structured_streaming.py
```
9.Running twitter.py to get the tweets in structured_streaming
```console
python3 twitter.py
```

