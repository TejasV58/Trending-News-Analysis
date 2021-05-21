import findspark
findspark.init('/opt/spark')

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from pyspark.streaming.kafka import KafkaUtils

if __name__=="__main__":

    # sc = SparkContext(appName="Kafka Spark Demo")
    # ssc = StreamingContext(sc,60)
    #message  = KafkaUtils.createDirect
    spark = SparkSession.builder.appName("Pyspark demo").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers",["localhost:9092"]).option("subscribe",["news"]).option("startingOffsets","latest").load()

    df.printSchema()
    df1 = df.selectExpr("CAST(value AS STRING )")

    orders_agg_write_stream = df1 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()
