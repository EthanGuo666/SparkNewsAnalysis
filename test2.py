import jieba.analyse as analyse
from pyspark.sql import SparkSession, Row, SQLContext
#from pyspark.mllib.feature import Word2Vec
from pyspark.ml.feature import Word2Vec

#import
# 启动spark
spark = SparkSession.builder.master("local") \
    .appName("test") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext

sent = ("a b " * 100 + "a c " * 10).split(" ")
doc = spark.createDataFrame([(sent,), (sent,)], ["sentence"])
doc.show()#truncate=False)
word2Vec = Word2Vec(vectorSize=5, seed=42, inputCol="sentence", outputCol="model")
model = word2Vec.fit(doc)
print("model:",type(model))
vectors = model.getVectors()
print(type(vectors))
print(vectors)
#vectors.show()
#model.getVectors().show()

#print(model)