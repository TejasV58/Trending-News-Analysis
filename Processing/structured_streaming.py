 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_TOPIC_NAME_CONS = "twitter"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars", "/home/sanika/Documents/spark-sql-kafka-0-10_2.11-2.4.0.jar,/home/sanika/Documents/kafka-clients-1.1.0.jar") \
        .config("spark.executor.extraClassPath", "/home/sanika/Documents/spark-sql-kafka-0-10_2.11-2.4.0.jar,/home/sanika/Documents/kafka-clients-1.1.0.jar") \
        .config("spark.executor.extraLibrary", "/home/sanika/Documents/spark-sql-kafka-0-10_2.11-2.4.0.jar,/home/sanika/Documents/kafka-clients-1.1.0.jar") \
        .config("spark.driver.extraClassPath", "/home/sanika/Documents/spark-sql-kafka-0-10_2.11-2.4.0.jar,/home/sanika/Documents/kafka-clients-1.1.0.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from testtopic
    twitter_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of twitter_df: ")
    twitter_df.printSchema()

    twitter_df1 = twitter_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the twitter data
    twitter_schema = StructType() \
        .add("id", StringType()) \
        .add("text", StringType()) \
        .add("retweets", StringType()) \
        .add("favorites", StringType())

    twitter_df2 = twitter_df1\
        .select(from_json(col("value"), twitter_schema).alias("twitter_columns"), "timestamp")

    twitter_df3 = twitter_df2.select("twitter_columns.*", "timestamp")

    print("Printing Schema of twitter_df3: ")
    twitter_df3.printSchema()

    # # Simple aggregate - find total_transaction_amount by grouping transaction_card_type
    # twitter_df4 = twitter_df3.groupBy("transaction_card_type")\
    #     .agg({'transaction_amount': 'sum'}).select("transaction_card_type", \
    #     col("sum(transaction_amount)").alias("total_transaction_amount"))

    # print("Printing Schema of twitter_df4: ")
    # twitter_df4.printSchema()

    # twitter_df4 = twitter_df4.withColumn("key", lit(100))\
    #                                                 .withColumn("value", concat(lit("{'transaction_card_type': '"), \
    #                                                 col("transaction_card_type"), lit("', 'total_transaction_amount: '"), \
    #                                                 col("total_transaction_amount").cast("string"), lit("'}")))

    # print("Printing Schema of twitter_df4:")
    # twitter_df4.printSchema()

    # # Write final result into console for debugging purpose
    # trans_detail_write_stream = twitter_df \
    #     .writeStream \
    #     .trigger(processingTime='1 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false")\
    #     .format("console") \
    #     .start()

    # # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    # trans_detail_write_stream_1 = twitter_df \
    #     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    #     .writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    #     .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
    #     .trigger(processingTime='1 seconds') \
    #     .outputMode("update") \
    #     .option("checkpointLocation", "file:///D://work//development//spark_structured_streaming_kafka//py_checkpoint") \
    #     .start()

    # trans_detail_write_stream.awaitTermination()

    print("PySpark Structured Streaming with Kafka Demo Application Completed.")