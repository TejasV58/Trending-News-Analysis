from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

#twitter setup
access_token = "1371406637362159622-ICkyarNJ3SuUAc0CVZb4PgtyQFUn2R"
access_token_secret =  "UcFJuNmWXjzOyWb9GZfhFSPJZaDplqVZbgxVo45dFwtxe"
consumer_key =  "FlIKebIx45zqbsMyBeBOuPlKT"
consumer_secret =  "hW1TgjUJ7PYDZLg39mkjGWul9s8JiDskhOuh78vYXui1QpimBP"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("trump", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="trump")

