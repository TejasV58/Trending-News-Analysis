import findspark
findspark.init('/opt/spark')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
import time

kafka_bootstrap_servers = 'localhost:9092'

def preprocessing(tweets):
    tweets = tweets.select(explode(split(tweets.text, "t_end")).alias("text"),col("score"))
    tweets = tweets.na.replace('', None)
    tweets = tweets.na.drop()
    tweets = tweets.withColumn('text', F.regexp_replace('text', r'http\S+', ''))
    tweets = tweets.withColumn('text', F.regexp_replace('text', '@\w+', ''))
    tweets = tweets.withColumn('text', F.regexp_replace('text', '#', ''))
    tweets = tweets.withColumn('text', F.regexp_replace('text', 'RT', ''))
    tweets = tweets.withColumn('text', F.regexp_replace('text', '\\n', ''))
    tweets = tweets.withColumn('text', F.regexp_replace('text', '[.!?\\-]', ' '))
    tweets = tweets.withColumn('text', F.regexp_replace('text', "[^ 'a-zA-Z0-9]", ''))

    stopwords = ["ourselves", "her", "between", "yourself", "but", "again", "there", "about", "once", "during", "out", "very", "having", "with", "they", "own", "an", "be", "some", "for", "do", "its", "yours", "such", "into", "of", "most", "itself", "other", "off", "is", "am", "or", "who", "as", "from", "him", "each", "the", "themselves", "until", "below", "are", "we", "these", "your", "his", "through", "don", "nor", "me", "were", "her", "more", "himself", "this", "down", "should", "our", "their", "while", "above", "both", "up", "to", "ours", "had", "she", "all", "no", "when", "at", "any", "before", "them", "same", "and", "been", "have", "in", "will", "on", "does", "yourselves", "then", "that", "because", "what", "over", "why", "so", "can", "did", "not", "now", "under", "he", "you", "herself", "has", "just", "where", "too", "only", "myself", "which", "those", "i", "after", "few", "whom", "being", "if", "theirs", "my", "against", "a", "by", "doing", "it", "how", "further", "was", "here", "than"]
    
    for stopword in stopwords:
        tweets = tweets.withColumn('text', F.regexp_replace('text', ' '+stopword+' ' , ' '))
    tweets = tweets.withColumn('text', F.regexp_replace('text', ' +', ' '))
    tweets.select(trim(col("text")))
    return tweets

def get_similarity(df):
    df = df.withColumn("word", F.split("word", ' '))
    hashingTF = HashingTF(inputCol="word", outputCol="tf")
    tf = hashingTF.transform(df)
    idf = IDF(inputCol="tf", outputCol="feature")
    modelidf = idf.fit(tf)
    tfidf = modelidf.transform(tf)

    normalizer = Normalizer(inputCol="feature", outputCol="norm")
    data = normalizer.transform(tfidf)
    
    mat = IndexedRowMatrix(
    data.select("ID", "norm")\
        .rdd.map(lambda row: IndexedRow(row.ID, row.norm.toArray()))).toBlockMatrix()
    dot = mat.multiply(mat.transpose())
    dot.toLocalMatrix().toArray()
    collected = df.select('word').toPandas()
    list_headlines = list(collected['word'])
    df = pd.DataFrame(dot, columns=list_headlines)
    sdf = spark.createDataFrame(df)
    return sdf

    
if __name__ == "__main__":
    print("Stream Data Processing Application Started ...\n")
    spark = SparkSession.builder.appName("PySpark Structured Streaming with Kafka").master("local[*]").getOrCreate()
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark.sparkContext.setLogLevel("ERROR")

     # Construct a streaming DataFrame that reads from headlines from newsapi, websearch api and inshorts

    headlines_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", "headlines").option("startingOffsets", "latest").load()
    
    headlines_df1 = headlines_df.selectExpr("CAST(value AS STRING)")

    # Define a schema for headlines

    headlines_schema = StructType().add("title", StringType())

    headlines_df2 = headlines_df1.select(from_json(col("value"), headlines_schema).alias("headlines_columns"))

    headlines_final_df = headlines_df2.select("headlines_columns.*")
 

    # Construct a streaming DataFrame that reads from twitter

    twitter_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", "twitter").option("startingOffsets", "latest").load()

    twitter_df1 = twitter_df.selectExpr("CAST(value AS STRING)")

    # Define a schema for twitter

    twitter_schema = StructType().add("id", StringType()).add("text", StringType()).add("score", IntegerType())

    twitter_df2 = twitter_df1.select(from_json(col("value"), twitter_schema).alias("twitter_columns"))

    twitter_df3 = twitter_df2.select("twitter_columns.*")
 
    twitter_final_df = preprocessing(twitter_df3)

    # Write final result into console for debugging purpose

    query_headlines = headlines_final_df.writeStream.trigger(processingTime='10 seconds').outputMode("update").option("truncate", "true").format("console").start()

    query_tweets = twitter_final_df.writeStream.trigger(processingTime='5 seconds').outputMode("update").option("truncate", "true").format("console").start()
    
    spark.streams.awaitAnyTermination()

    print("Stream Data Processing Application Completed.")