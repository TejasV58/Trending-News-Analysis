import findspark
findspark.init('/opt/spark')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import time

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import expr

import sparknlp
from sparknlp.annotator import *
from sparknlp.base import *
from sparknlp.pretrained import PretrainedPipeline

from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.feature import Normalizer
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pathlib import Path

PROCESSING_DIR = Path(__file__).resolve().parent

kafka_topic_name = "twitter"
kafka_bootstrap_servers = 'localhost:9092'

def preprocessing(tweets):
    tweets = tweets.select(col("id"),col("text").alias("original_text"),explode(split(tweets.text, "t_end")).alias("text"), col("score"))
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

def find_similarity(data):
    tweet_df = data.filter(data.type == "tweets")
    headline_df = data.filter(data.type == "headlines")
    dot_udf = F.udf(lambda x,y: float(x.dot(y)), DoubleType())
    joined_df = headline_df.alias('headlines').join(tweet_df.alias('tweets')).select(
        F.col("headlines.original_text").alias("headline_text"),
        F.col("tweets.original_text").alias("tweets_text"),
        dot_udf("headlines.norm", "tweets.norm").alias("similarity_score"))
    return joined_df


def update_static_df(batch_df, static_df):

    join_df = static_df.union(batch_df)

    columns = ['id', 'original_text', 'score', 'type', 'norm']
    vals = [("1#2#3#4#5#6#", "hello hello ", 0,"tweets","hello hello"),("6#5#4#3#2#1#", "hello hello", 0,"headlines","hello hello")]
    empty_df = spark.createDataFrame(vals, columns)
    df = join_df.union(empty_df)

    df = join_df.withColumn("text", F.split("text", ' '))
    merged_tfidf = light_pipeline.transform(df)
    columns_to_drop = ['text','tf','feature']
    merged_tfidf = merged_tfidf.drop(*columns_to_drop)

    similairty_scores_df = find_similarity(merged_tfidf)
    similairty_scores_df = similairty_scores_df.filter(similairty_scores_df.similarity_score > 0.1)
    similairty_scores_df.show(30, False)
    return join_df


if __name__ == "__main__":
    print("Stream Data Processing Application Started ...\n")
    spark = SparkSession.builder.appName("PySpark Structured Streaming with Kafka").master("local[*]").getOrCreate()
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    print("\n\n=====================================================================")
    print("###########  Stream Data Processing Application Started  ############")
    print("=====================================================================\n\n")
    spark.sparkContext.setLogLevel("ERROR")

    headlines_path = PROCESSING_DIR.joinpath('headlines')
    
    headlines_schema = StructType([
    StructField("id", StringType(), True),
    StructField("original_text", StringType(), True),
    StructField("text", StringType(), True),
    StructField("score", IntegerType(), True),
    ])

    headlines_df = spark.read.csv(str(headlines_path)+"/part-*.csv",header=False,schema=headlines_schema)
    headlines_df = headlines_df.withColumn("type",lit("headlines"))
    

    ############################  TF-IDF PIPELINE  ###############################

    hashingTF = HashingTF(inputCol="text", outputCol="tf")
    idf = IDF(inputCol="tf", outputCol="feature")
    normalizer = Normalizer(inputCol="feature", outputCol="norm")

    nlp_pipeline = Pipeline(stages=[
        hashingTF, 
        idf,
        normalizer
    ])
    
    empty_df = spark.createDataFrame([[['']]]).toDF('text')
    pipeline_model = nlp_pipeline.fit(empty_df)
    light_pipeline = LightPipeline(pipeline_model)

    ###################  Construct a streaming DataFrame for twitter  #########################

    twitter_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", "twitter").option("startingOffsets", "latest").load()

    twitter_df1 = twitter_df.selectExpr("CAST(value AS STRING)")

    ################# Define a schema for twitter  #############################

    twitter_schema = StructType().add("id", StringType()).add("text", StringType()).add("score", IntegerType())

    twitter_df2 = twitter_df1.select(from_json(col("value"), twitter_schema).alias("twitter_columns"))

    twitter_df3 = twitter_df2.select("twitter_columns.*")
    twitter_df4 = preprocessing(twitter_df3)
    twitter_final_df = twitter_df4.withColumn("type",lit("tweets"))

    #################### Write final result into console for debugging purpose  ##########################
    
    merge_query = twitter_final_df.writeStream\
        .trigger(processingTime='2 seconds')\
        .outputMode("update")\
        .format("console")\
        .foreachBatch(lambda each_tweet_df, batchId: update_static_df(each_tweet_df, headlines_df))\
        .start()


    spark.streams.awaitAnyTermination()

    print("Stream Data Processing Application Completed.")