import findspark
findspark.init('/opt/spark')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time

kafka_topic_name = "twitter"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    print("Stream Data Processing Application Started ...\n")
    spark = SparkSession.builder.appName("PySpark Structured Streaming with Kafka Demo").master("local[*]").config("spark.jars","/home/sanika/Documents/spark-sql-kafka-0-10_2.11-2.4.0.jar","/home/sanika/Documents/kafka-clients-1.1.0.jar").config("spark.executor.extraClassPath","/home/sanika/Documents/spark-sql-kafka-0-10_2.11-2.4.0.jar","/home/sanika/Documents/kafka-clients-1.1.0.jar").config("spark.executor.extraLibrary","/home/sanika/Documents/spark-sql-kafka-0-10_2.11-2.4.0.jar","/home/sanika/Documents/kafka-clients-1.1.0.jar").config("spark.driver.extraClassPath","/home/sanika/Documents/spark-sql-kafka-0-10_2.11-2.4.0.jar","/home/sanika/Documents/kafka-clients-1.1.0.jar").getOrCreate()
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from twitter
    twitter_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", kafka_topic_name).option("startingOffsets", "latest").load()

    # print("Printing Schema of twitter_df: ")
    # twitter_df.printSchema()

    twitter_df1 = twitter_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the twitter data
    twitter_schema = StructType() \
        .add("id", StringType()) \
        .add("text", StringType()) \
        .add("retweets", StringType()) \
        .add("favorites", StringType())

    twitter_df2 = twitter_df1\
        .select(from_json(col("value"), twitter_schema)\
        .alias("twitter_columns"), "timestamp")

    twitter_df3 = twitter_df2.select("twitter_columns.*", "timestamp")
    print("Printing Schema of twitter_df3: ")
    twitter_df3.printSchema()

    # # Simple aggregate - find total_order_amount by grouping country, city
    # twitter_df4 = twitter_df3.groupBy("order_country_name", "order_city_name") \
    #     .agg({'order_amount': 'sum'}) \
    #     .select("order_country_name", "order_city_name", col("sum(order_amount)") \
    #     .alias("total_order_amount"))

    # print("Printing Schema of twitter_df4: ")
    # twitter_df4.printSchema()

    # # Write final result into console for debugging purpose
    # twitter_agg_write_stream = twitter_df4 \
    #     .writeStream \
    #     .trigger(processingTime='5 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false")\
    #     .format("console") \
    #     .start()

    # twitter_agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")