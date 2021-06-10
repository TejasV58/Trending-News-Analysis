import findspark
findspark.init('/opt/spark')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import time

from pathlib import Path

from preprocessing import preprocessing
from Tfidf_Pipeline import Tfidf_Pipeline

PROCESSING_DIR = Path(__file__).resolve().parent

kafka_topic_name = "twitter"
kafka_bootstrap_servers = 'localhost:9092'

def find_similarity(data):
    tweet_df = data.filter(data.type == "tweets")
    headline_df = data.filter(data.type == "headlines")
    dot_udf = F.udf(lambda x,y: float(x.dot(y)), DoubleType())
    joined_df = headline_df.alias('headlines').join(tweet_df.alias('tweets')).select(
        F.col("tweets._id").alias("tweet_id"),
        F.col("headlines._id").alias("headline_id"),
        F.col("headlines.original_text").alias("headline_text"),
        F.col("tweets.original_text").alias("tweet_text"),
        F.col("tweets.score").alias("tweet_score"),
        F.col("headlines.score").alias("headline_score"),
        dot_udf("headlines.norm", "tweets.norm").alias("similarity_score"))

    return joined_df


def update_static_df(batch_df):

    headlines_df = spark.read.format("mongo")\
        .option("uri","mongodb+srv://sanikatejas:10thmay@cluster0.095pi.mongodb.net/TrendingNewsDatabase?retryWrites=true&w=majority")\
        .option("database", "TrendingNewsDatabase")\
        .option("collection", "Headlines")\
        .load()

    static_df = headlines_df.withColumn("type",lit("headlines"))
    static_df = static_df.select(col("_id"),col("original_text"),col("text"),col("score"),col("source"),col("type"))
    static_df.show()
    batch_df.show()

    join_df = static_df.union(batch_df)

    df = join_df.withColumn("text", F.split("text", ' '))
    merged_tfidf = light_pipeline.transform(df)
    columns_to_drop = ['text','tf','feature']
    merged_tfidf = merged_tfidf.drop(*columns_to_drop)

    similairty_scores_df = find_similarity(merged_tfidf)
    similairty_scores_df = similairty_scores_df.filter(similairty_scores_df.similarity_score > 0.25)
    similairty_scores_df = similairty_scores_df.dropDuplicates(['tweet_id'])
    similairty_scores_df.show(30, False)

    merged_scores = similairty_scores_df.withColumn('score', col('headline_score')+col('tweet_score'))
    columns_to_drop = ['tweet_id','tweet_score','tweet_text','headline_score','similarity_score']
    merged_scores = merged_scores.drop(*columns_to_drop)
    merged_scores_grp = merged_scores.groupBy("headline_id").sum("score")
    merged_scores_grp.show(50,False)

    similar_headline_ids = merged_scores_grp.select(col("headline_id"))
    headline_filtered = static_df.join(similar_headline_ids, static_df._id == similar_headline_ids.headline_id,"left_anti")
    headline_filtered = headline_filtered.select(col("_id"),col("original_text"),col("text"),col("score"),col("source"))

    matching_headlines = static_df.join(merged_scores_grp , static_df._id == merged_scores_grp.headline_id)
    columns_to_drop = ['headline_id','score']
    matching_headlines = matching_headlines.drop(*columns_to_drop)
    matching_headlines = matching_headlines.withColumnRenamed('sum(score)','score')
    matching_headlines = matching_headlines.select(col("_id"),col("original_text"),col("text"),col("score"),col("source"))
    matching_headlines.show(20,False)

    new_headlines_df = matching_headlines.union(headline_filtered)
    new_headlines_df.show()

    new_headlines_df.write\
        .format(source="mongo")\
        .mode(saveMode="append")\
        .option("uri", "mongodb+srv://sanikatejas:10thmay@cluster0.095pi.mongodb.net/TrendingNewsDatabase?retryWrites=true&w=majority")\
        .option("database", "TrendingNewsDatabase")\
        .option("collection", "Headlines")\
        .save()

    print("\n\n=====================================================================")
    print("###############  Headlines updated in Mongo Database ################")
    print("=====================================================================\n\n")

    return new_headlines_df


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("PySpark Structured Streaming with Kafka")\
        .config("spark.mongodb.input.uri", "mongodb+srv://sanikatejas:10thmay@cluster0.095pi.mongodb.net/TrendingNewsDatabase?retryWrites=true&w=majority")\
        .config("spark.mongodb.output.uri", "mongodb+srv://sanikatejas:10thmay@cluster0.095pi.mongodb.net/TrendingNewsDatabase?retryWrites=true&w=majority")\
        .master("local[*]")\
        .getOrCreate()

    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    print("\n\n=====================================================================")
    print("###########  Stream Data Processing Application Started  ############")
    print("=====================================================================\n\n")
    spark.sparkContext.setLogLevel("ERROR")

    headlines_path = PROCESSING_DIR.joinpath('headlines')
    
    headlines_schema = StructType([
        StructField("_id", StringType(), True),
        StructField("original_text", StringType(), True),
        StructField("text", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("source", StringType(),True)
    ])

    # headlines_df = spark.read.format("mongo").option("uri","mongodb+srv://sanikatejas:10thmay@cluster0.095pi.mongodb.net/TrendingNewsDatabase.Headlines").load()
    # headlines_df.show()
    # headlines_df = headlines_df.withColumn("type",lit("headlines"))
    
    #============================================================================================#
    ###################################  TF-IDF PIPELINE  ########################################
    #============================================================================================#

    light_pipeline = Tfidf_Pipeline(spark)

    #============================================================================================#
    ######################  Construct a streaming DataFrame for twitter  #########################
    #============================================================================================#

    twitter_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
        .option("subscribe", "twitter")\
        .option("startingOffsets", "latest")\
        .load()

    twitter_df1 = twitter_df.selectExpr("CAST(value AS STRING)")

    #============================================================================================#
    #############################   Define a schema for twitter   ################################
    #============================================================================================#

    twitter_schema = StructType()\
        .add("_id", StringType())\
        .add("text", StringType())\
        .add("score", IntegerType())\
        .add("source", StringType())

    twitter_df2 = twitter_df1.select(from_json(col("value"), twitter_schema).alias("twitter_columns"))
    twitter_df3 = twitter_df2.select("twitter_columns.*")
    twitter_df4 = preprocessing(twitter_df3)
    twitter_final_df = twitter_df4.withColumn("type",lit("tweets"))

    #============================================================================================#
    ################   Write final result into console for debugging purpose   ###################
    #============================================================================================#

    merge_query = twitter_final_df.writeStream\
        .trigger(processingTime='2 seconds')\
        .outputMode("update")\
        .format("console")\
        .foreachBatch(lambda each_tweet_df, batchId: update_static_df(each_tweet_df))\
        .start()

    spark.streams.awaitAnyTermination()

    print("Stream Data Processing Application Completed.")