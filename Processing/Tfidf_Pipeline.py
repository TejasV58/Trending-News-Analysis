
from pyspark.ml import Pipeline
import pyspark.sql.functions as F

from sparknlp.annotator import *
from sparknlp.base import *

from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.feature import Normalizer

def Tfidf_Pipeline(spark):

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

    return light_pipeline