import json
import os
import pandas as pd
import numpy as np


spark = sparknlp.start()

MODEL_NAME = "tfhub_use"
os.environ['MODEL_NAME'] = MODEL_NAME

# Transforms the input text into a document usable by the SparkNLP pipeline.
document_assembler = DocumentAssembler()
document_assembler.setInputCol('text')
document_assembler.setOutputCol('document')

# Separates the text into individual tokens (words and punctuation).
tokenizer = Tokenizer()
tokenizer.setInputCols(['document'])
tokenizer.setOutputCol('token')

# Encodes the text as a single vector representing semantic features.
sentence_encoder = UniversalSentenceEncoder.pretrained(name=MODEL_NAME)
sentence_encoder.setInputCols(['document', 'token'])
sentence_encoder.setOutputCol('sentence_embeddings')

nlp_pipeline = Pipeline(stages=[
    document_assembler, 
    tokenizer,
    sentence_encoder
])

# Fit the model to an empty data frame so it can be used on inputs.
empty_df = spark.createDataFrame([['']]).toDF('text')
pipeline_model = nlp_pipeline.fit(empty_df)
light_pipeline = LightPipeline(pipeline_model)

