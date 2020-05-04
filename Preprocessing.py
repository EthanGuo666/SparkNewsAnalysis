import pandas as pd
import jieba.analyse as analyse
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.types import StringType
from pyspark.ml.feature import Word2Vec
from pyspark.ml import Pipeline


# 启动spark
spark = SparkSession.builder.master("local") \
    .appName("Preprocessing") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext

# 读数据文件
file_df = spark.read.csv("data/origin_news_data.csv", sep=',', header=True, encoding='GBK')

# file_list是一个[Row(title='xxx', main_body='xxx', link='xxx'), ..., Row()]
file_list = file_df.collect()
# print(file_list)

# 设定CPU核数
data_rdd = sc.parallelize(file_list, 2)
# type(data_rdd) = <class 'pyspark.rdd.RDD'>

# 把标题和正文字段连接成一个长串，然后分词
def word_seg(single_news_piece):
    title_string = str(single_news_piece['title'])
    main_body_string = str(single_news_piece['main_body'])
    cmb_string = title_string + main_body_string
    stopwords=pd.read_csv("data/stopwords.txt", index_col=False, quoting=3,
                          sep="\t", names=['stopword'], encoding='utf-8')
    stopwords=stopwords['stopword'].values
    # extract key words, topK is the K biggest values of TF-IDF index.
    words = analyse.extract_tags(cmb_string, topK=120, withWeight=False, allowPOS=())
    words = list(filter(lambda x:x not in stopwords, words))
    return Row(title = str(single_news_piece['title']),
               main_body = str(single_news_piece['main_body']),
               link = str(single_news_piece['link']),
               words = words)

word_seg_rdd = data_rdd.map(word_seg)
# rdd转dataframe
# 对word_seg_df = SQLContext(sc).createDataFrame(word_seg_rdd)
# 对word_seg_df = word_seg_rdd.toDF()

print(word_seg_rdd.collect())

#word_seg_df = SQLContext(sc).createDataFrame(word_seg_rdd)
word_seg_df = word_seg_rdd.toDF()
word_seg_df.show()

# 把文档向量化，每条新闻分成的词是一个文档
w2v = Word2Vec(vectorSize=300, minCount=0, inputCol="words", outputCol="features")
doc2vec_pipeline = Pipeline(stages=[w2v])
doc2vec_model = doc2vec_pipeline.fit(word_seg_df)
doc2vecs_df = doc2vec_model.transform(word_seg_df)
doc2vecs_df.select('link','features').show(doc2vecs_df.count())

# 8个分类中文词，先放到Dataframe里
categories = ['物联网', '云计算', '大数据', '人工智能', '工业互联网', '信息安全', '智能制造', '集成电路']
df = spark.createDataFrame(categories, StringType())
categories_df = df.withColumnRenamed("value", "categories")
categories_df.show()

# DataFrame转rdd
categories_list = categories_df.collect()
categories_rdd = sc.parallelize(categories_list, 2)

def center_wordlist(categories_string):
    return Row(center = list(categories_string))

center_rdd = categories_rdd.map(center_wordlist)
print(center_rdd.collect())
center_df = center_rdd.toDF()
center_df.show()

categories_w2v = Word2Vec(vectorSize=300, minCount=0, inputCol="center", outputCol="features")
cat2vec_pipeline = Pipeline(stages=[categories_w2v])
cat2vec_model = cat2vec_pipeline.fit(center_df)
cat2vecs_df = cat2vec_model.transform(center_df)
cat2vecs_df.show()