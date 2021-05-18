import findspark
findspark.init('/opt/spark')

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__=="__main__":

    sc = SparkContext(appName="Kafka Spark Demo")
    ssc = StreamingContext(sc,60)
    #message  = KafkaUtils.createDirect