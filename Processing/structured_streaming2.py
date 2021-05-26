import findspark
findspark.init('/opt/spark')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import time

from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.feature import Normalizer
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix

import os
import json
import pandas as pd
import numpy as np

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sparknlp
from sparknlp.annotator import *
from sparknlp.base import *
from sparknlp.pretrained import PretrainedPipeline

spark = sparknlp.start()

#MODEL_NAME = "tfhub_use"

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


# Fit the model to an empty data frame so it can be used on inputs.

# def define_pipeline():
#     document_assembler = DocumentAssembler()
#     document_assembler.setInputCol('word')
#     document_assembler.setOutputCol('document')

#     # Separates the text into individual tokens (words and punctuation).
#     tokenizer = Tokenizer()
#     tokenizer.setInputCols(['document'])
#     tokenizer.setOutputCol('token')

#     # Encodes the text as a single vector representing semantic features.
#     sentence_encoder = UniversalSentenceEncoder.pretrained(name=MODEL_NAME)
#     sentence_encoder.setInputCols(['document', 'token'])
#     sentence_encoder.setOutputCol('sentence_embeddings')
    

#     nlp_pipeline = Pipeline(stages=[
#         document_assembler,
#         tokenizer,
#         sentence_encoder
#     ])

#     empty_df = spark.createDataFrame([['']]).toDF('word')
#     pipeline_model = nlp_pipeline.fit(empty_df)
#     light_pipeline = LightPipeline(pipeline_model)
#     return light_pipeline

# def get_similarity(df,light_pipeline):
#     df = df.withColumn("word", F.split("word", ' '))
#     result = light_pipeline.transform(df)
#     embeddings = []
#     for r in result.collect():
#         embeddings.append(r.sentence_embeddings[0].embeddings)
#     embeddings_matrix = np.array(embeddings)
#     similarities = np.matmul(embeddings_matrix, embeddings_matrix.transpose())
#     collected = df.select('word').toPandas()
#     list_headlines = list(collected['word'])
#     df = pd.DataFrame(similarities, columns=list_headlines)
#     sdf = spark.createDataFrame(df)
#     return sdf


if __name__ == "__main__":

    print("Stream Data Processing Application Started ....\n")
    spark = SparkSession.builder.config('spark.jars', 'com.johnsnowlabs.nlp:spark-nlp_2.11:2.2.2').appName("PySpark Structured Streaming with Kafka Demo").master("local[*]").getOrCreate()

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
    tweet = get_similarity(tweet)

    # Write final result into console for debugging purpose

    query = tweet.writeStream.trigger(processingTime='10 seconds').outputMode("update").option("truncate", "false").format("console").start()
    query.awaitTermination()

    print("Stream Data Processing Application Completed.")
    
