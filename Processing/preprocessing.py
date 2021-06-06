from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F


def preprocessing(data):
    data = data.select(col("_id"),col("text").alias("original_text"),explode(split(data.text, "t_end")).alias("text"), col("score"),col("source"))
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
