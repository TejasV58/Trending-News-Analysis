import findspark
findspark.init('/opt/spark')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
import time

kafka_topic_name = "twitter"
kafka_bootstrap_servers = 'localhost:9092'

def preprocessing(tweets):
    words = tweets.select(explode(split(tweets.text, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', '\\n', ''))
    # words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    words = words.withColumn('word', F.regexp_replace('word', "[^ 'a-zA-Z0-9]", ''))
    return words
if __name__ == "__main__":
    print("Stream Data Processing Application Started ...\n")
    spark = SparkSession.builder.appName("PySpark Structured Streaming with Kafka Demo").master("local[*]").getOrCreate()
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from twitter
    twitter_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", kafka_topic_name).option("startingOffsets", "latest").load()

    twitter_df1 = twitter_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the twitter data
    twitter_schema = StructType().add("id", StringType()).add("text", StringType()).add("retweets", StringType()).add("favorites", StringType())

    twitter_df2 = twitter_df1.select(from_json(col("value"), twitter_schema).alias("twitter_columns"), "timestamp")

    twitter_df3 = twitter_df2.select("twitter_columns.*", "timestamp")
 
    tweet = preprocessing(twitter_df3)

    # Write final result into console for debugging purpose

    query = tweet.writeStream.trigger(processingTime='10 seconds').outputMode("update").option("truncate", "false").format("console").start()
    query.awaitTermination()

    print("Stream Data Processing Application Completed.")