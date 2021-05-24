import findspark
findspark.init('/opt/spark')

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__=="__main__":

    sc = SparkContext(appName="Kafka Spark Demo")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc,5)
    # message = KafkaUtils.createDirectStream(ssc, topics=['twitter'],kafkaParams= {"metadata.broker.list":"localhost:9092"})

    # words = message.map(lambda x: x[1]).flatMap(lambda x: x.split(" "))

    # wordcount = words.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)

    # wordcount.pprint()

    # ssc.start()
    # ssc.awaitTermination()
    # sc.stop()

    
