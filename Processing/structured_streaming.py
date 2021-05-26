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


kafka_topic_name = "twitter"
kafka_bootstrap_servers = 'localhost:9092'

def preprocessing(tweets):
    tweets = tweets.select(col("id"),explode(split(tweets.text, "t_end")).alias("text"), col("score"))
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

def find_similarity(data,spark):
    dot_udf = F.udf(lambda x,y: float(x.dot(y)), DoubleType())
    data = data.alias("i").join(data.alias("j"), expr(F.col("i.ID") < F.col("j.ID")))\
    .select(
        F.col("i.ID").alias("i"), 
        F.col("j.ID").alias("j"), 
        dot_udf("i.norm", "j.norm").alias("dot"))
    return data


    
if __name__ == "__main__":
    print("Stream Data Processing Application Started ...\n")
    spark = SparkSession.builder.appName("PySpark Structured Streaming with Kafka").master("local[*]").getOrCreate()
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    print("\n\n=====================================================================")
    print("Stream Data Processing Application Started.....")
    print("=====================================================================\n\n")
    spark.sparkContext.setLogLevel("ERROR")

     # Construct a streaming DataFrame that reads from headlines from newsapi, websearch api and inshorts

    headlines_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", "headlines").option("startingOffsets", "latest").load()
    
    headlines_df1 = headlines_df.selectExpr("CAST(value AS STRING)")

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

    ##############  streaming DataFrame for headlines from newsapi, websearch api and inshorts  #############

    headlines_schema = StructType().add("title", StringType())

    headlines_df2 = headlines_df1.select(from_json(col("value"), headlines_schema).alias("headlines_columns"))

    headlines_df1 = headlines_df.selectExpr("CAST(value AS STRING)")
    headlines_schema = StructType().add("title", StringType())      # Define a schema for headlines
    headlines_df2 = headlines_df1\
        .select(from_json(col("value"), headlines_schema)
        .alias("headlines_columns"))
    headlines_df3 = headlines_df2.select("headlines_columns.*")
    headlines_df4 = headlines_df3.withColumn("score",lit(100))

    ###################  Construct a streaming DataFrame for twitter  #########################

    twitter_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", "twitter").option("startingOffsets", "latest").load()

    twitter_df1 = twitter_df.selectExpr("CAST(value AS STRING)")

    ################# Define a schema for twitter  #############################

    twitter_schema = StructType().add("id", StringType()).add("text", StringType()).add("score", IntegerType())

    twitter_df2 = twitter_df1.select(from_json(col("value"), twitter_schema).alias("twitter_columns"))

    twitter_df3 = twitter_df2.select("twitter_columns.*")
    twitter_final_df = preprocessing(twitter_df3)

    #twitter_headlines_df = twitter_final_df.union(df_2)
    df = twitter_final_df.withColumn("text", F.split("text", ' '))
    twitter_tfidf = light_pipeline.transform(df)

    #if len(twitter_tfidf.count()) != 0:
    # similarities = find_similarity(twitter_tfidf,spark)

    # query_tweets = similarities.writeStream\
    #     .trigger(processingTime='5 seconds')\
    #     .outputMode("append")\
    #     .option("truncate", "true")\
    #     .format("console")\
    #     .start()
    
    #################### Write final result into console for debugging purpose  ##########################
    
    query_headlines = headlines_df4\
        .writeStream.trigger(processingTime='5 seconds')\
        .outputMode("update")\
        .option("truncate", "false")\
        .format("console")\
        .start()
        
    query_tweets = twitter_tfidf.writeStream\
        .trigger(processingTime='5 seconds')\
        .outputMode("update")\
        .option("truncate", "true")\
        .format("console")\
        .start()    


    

    spark.streams.awaitAnyTermination()

    print("Stream Data Processing Application Completed.")