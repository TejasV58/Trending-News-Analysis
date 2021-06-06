import findspark
findspark.init('/opt/spark')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

from pathlib import Path
import time

from preprocessing import preprocessing
from Tfidf_Pipeline import Tfidf_Pipeline

import pymongo
from pymongo import MongoClient

PROCESSING_DIR = Path(__file__).resolve().parent

kafka_topic_name = "headlines"
kafka_bootstrap_servers = 'localhost:9092'

def find_similarity(data):

    inshorts_df = data.filter(data.source == "inshorts")
    newsapi_df = data.filter(data.source == "newsapi")
    websearch_df = data.filter(data.source == "websearch")

    dot_udf = F.udf(lambda x,y: float(x.dot(y)), DoubleType())
    joined_df = inshorts_df.alias('inshorts').join(newsapi_df.alias('newsapi')).select(
        F.col("inshorts._id").alias("inshorts_id"),
        F.col("newsapi._id").alias("newsapi_id"),
        F.col("inshorts.original_text").alias("inshorts_text"),
        F.col("newsapi.original_text").alias("newsapi_text"),
        F.col("inshorts.score").alias("inshorts_score"),
        F.col("newsapi.score").alias("newsapi_score"),
        F.col("inshorts.norm").alias("inshorts_norm"),
        F.col("inshorts.source").alias("source"),
        dot_udf("inshorts.norm", "newsapi.norm").alias("similarity_score"))

    joined_df = joined_df.filter(joined_df.similarity_score > 0.3)
    
    inshorts_similar_df = joined_df.select(col("inshorts_id"))
    inshorts_filtered = inshorts_df.join(inshorts_similar_df, inshorts_df._id == inshorts_similar_df.inshorts_id,"left_anti")

    newsapi_similar_df = joined_df.select(col("newsapi_id"))
    newsapi_filtered = newsapi_df.join(newsapi_similar_df, newsapi_df._id == newsapi_similar_df.newsapi_id,"left_anti")

    joined_df = joined_df.withColumn('score', col('inshorts_score')+col('newsapi_score'))
    joined_df = joined_df.select(col("inshorts_id").alias("_id"),col("inshorts_text").alias("original_text"),col("score"),col("source"),col("inshorts_norm").alias("norm"))
    joined_df = joined_df.union(inshorts_filtered)
    joined_df = joined_df.union(newsapi_filtered)
    
    joined_final_df = joined_df.alias('joined').join(websearch_df.alias('websearch')).select(
        F.col("joined._id").alias("joined_id"),
        F.col("websearch._id").alias("websearch_id"),
        F.col("joined.original_text").alias("joined_text"),
        F.col("websearch.original_text").alias("websearch_text"),
        F.col("joined.score").alias("joined_score"),
        F.col("websearch.score").alias("websearch_score"),
        F.col("joined.norm").alias("norm"),
        F.col("joined.source").alias("source"),
        dot_udf("joined.norm", "websearch.norm").alias("similarity_score"))

    joined_final_df = joined_final_df.filter(joined_final_df.similarity_score > 0.3)

    joined_similar_df = joined_final_df.select(col("joined_id"))
    joined_filtered = joined_df.join(joined_similar_df, joined_df._id == joined_similar_df.joined_id,"left_anti")

    websearch_similar_df = joined_final_df.select(col("websearch_id"))
    websearch_filtered = websearch_df.join(websearch_similar_df, websearch_df._id == websearch_similar_df.websearch_id,"left_anti")

    joined_final_df = joined_final_df.withColumn('score', col('joined_score')+col('websearch_score'))
    joined_final_df = joined_final_df.select(col("joined_id").alias("_id"),col("joined_text").alias("original_text"),col("score"),col("source"),col("norm"))
    joined_final_df = joined_final_df.union(joined_filtered)
    joined_final_df = joined_final_df.union(websearch_filtered)
    joined_final_df = joined_final_df.limit(150)
    joined_final_df = joined_final_df.drop('norm')
    joined_final_df = joined_final_df.withColumnRenamed('original_text','text')

    return joined_final_df


def find_similiar_headlines(df):

    df = df.withColumn("text", F.split("text", ' '))
    tfidf = light_pipeline.transform(df)
    columns_to_drop = ['text','tf','feature']
    tfidf = tfidf.drop(*columns_to_drop)

    similairty_scores_df = find_similarity(tfidf)
    similairty_scores_df.show()
    final_df = preprocessing(similairty_scores_df)
    final_df.write\
    .format(source = "mongo")\
    .mode(saveMode = "append")\
    .option("uri","mongodb+srv://sanikatejas:10thmay@cluster0.095pi.mongodb.net/TrendingNewsDatabase?retryWrites=true w=majority")\
    .option("database","TrendingNewsDatabase")\
    .option("collection","Headlines")\
    .save()
    final_df.show()
    return df


if __name__ == "__main__":
    spark = SparkSession.builder.appName("PySpark Structured Streaming with Kafka for headlines")
    .config("spark.mongodb.input.uri", "mongodb+srv://sanikatejas:10thmay@cluster0.095pi.mongodb.net/TrendingNewsDatabase?retryWrites=true&w=majority")\
    .config("spark.mongodb.output.uri", "mongodb+srv://sanikatejas:10thmay@cluster0.095pi.mongodb.net/TrendingNewsDatabase?retryWrites=true&w=majority")\
    .master("local[*]").getOrCreate()
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    print("\n\n=====================================================================")
    print("#########  Stream Headlines Processing Application Started ##########")
    print("=====================================================================\n\n")
    
    spark.sparkContext.setLogLevel("ERROR")

    ############################  TF-IDF PIPELINE  ###############################

    light_pipeline = Tfidf_Pipeline(spark)

    ##############  streaming DataFrame for headlines from newsapi, websearch api and inshorts  #############

    headlines_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
        .option("subscribe", "headlines")\
        .option("startingOffsets", "latest")\
        .option("maxOffsetsPerTrigger",500)\
        .load()

    headlines_schema = StructType()\
        .add("_id", StringType())\
        .add("text", StringType())\
        .add("source", StringType())
        

    headlines_df1 = headlines_df.selectExpr("CAST(value AS STRING)")     
    headlines_df2 = headlines_df1\
        .select(from_json(col("value"), headlines_schema)
        .alias("headlines_columns"))
    headlines_df3 = headlines_df2.select("headlines_columns.*")
    headlines_df4 = headlines_df3.withColumn("score",lit(100))

    final_df = preprocessing(headlines_df4)

    #################### Write final result into console for debugging purpose  ##########################

    headlines_path = PROCESSING_DIR.joinpath('headlines')
    
    query_headlines = final_df \
        .writeStream.trigger(processingTime='10 seconds')\
        .outputMode("update")\
        .option("truncate", "true")\
        .format("console")\
        .foreachBatch(lambda batch_headline_df, batchId: find_similiar_headlines(batch_headline_df))\
        .start()
    
    spark.streams.awaitAnyTermination()

    print("\n\n=====================================================================")
    print("Stream Headlines Processing Application Completed.")
    print("=====================================================================\n\n")
