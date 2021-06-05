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


# cluster = MongoClient ("mongodb+srv://sanikatejas:10thmay@cluster0.095pi.mongodb.net/TrendingNewsDatabase?retryWrites=true&w=majority")

# db = cluster["TrendingNewsDatabase"]
# collection = db["Headlines"]

PROCESSING_DIR = Path(__file__).resolve().parent

kafka_topic_name = "headlines"
kafka_bootstrap_servers = 'localhost:9092'

# mongodb_host_name = "localhost"
# mongodb_port_no = "27017"
# mongodb_user_name = "admin"
# mongodb_password = "admin"
# mongodb_database_name = "TrendingNewsDatabase"
# mongodb_collection_name = "Headlines"
# mongo_uri="mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name

def preprocessing(data):
    data = data.select(col("id").alias("_id"),col("text").alias("original_text"),explode(split(data.text, "t_end")).alias("text"), col("score"))
    data = data.na.replace('', None)
    data = data.na.drop()
    data = data.withColumn('text', F.regexp_replace('text', r'http\S+', ''))
    data = data.withColumn('text', F.regexp_replace('text', '@\w+', ''))
    data = data.withColumn('text', F.regexp_replace('text', '#', ''))
    data = data.withColumn('text', F.regexp_replace('text', 'RT', ''))
    data = data.withColumn('text', F.regexp_replace('text', '\\n', ''))
    data = data.withColumn('text', F.regexp_replace('text', '[.!?\\-]', ' '))
    data = data.withColumn('text', F.regexp_replace('text', "[^ 'a-zA-Z0-9]", ''))

    stopwords = ["ourselves", "her", "between", "yourself", "but", "again", "there", "about", "once", "during", "out", "very", "having", "with", "they", "own", "an", "be", "some", "for", "do", "its", "yours", "such", "into", "of", "most", "itself", "other", "off", "is", "am", "or", "who", "as", "from", "him", "each", "the", "themselves", "until", "below", "are", "we", "these", "your", "his", "through", "don", "nor", "me", "were", "her", "more", "himself", "this", "down", "should", "our", "their", "while", "above", "both", "up", "to", "ours", "had", "she", "all", "no", "when", "at", "any", "before", "them", "same", "and", "been", "have", "in", "will", "on", "does", "yourselves", "then", "that", "because", "what", "over", "why", "so", "can", "did", "not", "now", "under", "he", "you", "herself", "has", "just", "where", "too", "only", "myself", "which", "those", "i", "after", "few", "whom", "being", "if", "theirs", "my", "against", "a", "by", "doing", "it", "how", "further", "was", "here", "than"]
    
    for stopword in stopwords:
        data = data.withColumn('text', F.regexp_replace('text', ' '+stopword+' ' , ' '))
    data = data.withColumn('text', F.regexp_replace('text', ' +', ' '))
    data.select(trim(col("text")))
    return data

def find_similarity(data,spark):
    dot_udf = F.udf(lambda x,y: float(x.dot(y)), DoubleType())
    data = data.alias("i").join(data.alias("j"), expr(F.col("i.ID") < F.col("j.ID")))\
    .select(
        F.col("i.ID").alias("i"), 
        F.col("j.ID").alias("j"), 
        dot_udf("i.norm", "j.norm").alias("dot"))
    return data

def store_mongo(headlines):
    headlines.write\
    .format(source = "mongo")\
    .mode(saveMode = "append")\
    .option("uri","mongodb+srv://sanikatejas:10thmay@cluster0.095pi.mongodb.net/TrendingNewsDatabase?retryWrites=true&w=majority")\
    .option("database","TrendingNewsDatabase")\
    .option("collection","Headlines")\
    .save()
    # headlines = [row.asDict() for row in headlines.collect()]
    # for headline in headlines:
    #     collection.insert_one(headline)

if __name__ == "__main__":
    
    spark = SparkSession.builder\
    .appName("PySpark Structured Streaming with Kafka for headlines")\
    .master("local[*]")\
    .config("spark.mongodb.input.uri", "mongodb+srv://sanikatejas:10thmay@cluster0.095pi.mongodb.net/TrendingNewsDatabase?retryWrites=true&w=majority")\
    .config("spark.mongodb.output.uri", "mongodb+srv://sanikatejas:10thmay@cluster0.095pi.mongodb.net/TrendingNewsDatabase?retryWrites=true&w=majority")\
    .getOrCreate()
    
    
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    print("\n\n=====================================================================")
    print("#########  Stream Headlines Processing Application Started ##########")
    print("=====================================================================\n\n")
    spark.sparkContext.setLogLevel("ERROR")

    
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
    headlines_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
        .option("subscribe", "headlines")\
        .option("startingOffsets", "latest")\
        .load()

    headlines_schema = StructType()\
        .add("id", StringType())\
        .add("text", StringType())
        

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
        .writeStream.trigger(processingTime='2 seconds')\
        .outputMode("update")\
        .option("truncate", "true")\
        .format("console")\
        .foreachBatch(lambda each_headline_df, batchId: store_mongo(each_headline_df))\
        .start()
    
    # query_csv = final_df \
    #     .writeStream.trigger(processingTime='2 seconds')\
    #     .format("csv")\
    #     .option("checkpointLocation", "checkpoint/")\
    #     .option("path", headlines_path)\
    #     .outputMode("append")\
    #     .start()

    # allfiles =  spark.read.option("header","false").csv(str(headlines_path)+"/part-*.csv")
    # allfiles.coalesce(1).write.format("csv").option("header", "false").save(str(headlines_path)+"/single_csv_file/")
        
    spark.streams.awaitAnyTermination()

    # file = open("sample.txt","r+")
    # file. truncate(0)
    # file. close()

    print("\n\n=====================================================================")
    print("Stream Headlines Processing Application Completed.")
    print("=====================================================================\n\n")
