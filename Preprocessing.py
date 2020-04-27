import jieba.analyse as analyse
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row, SQLContext

# 启动spark
spark = SparkSession.builder.master("local") \
    .appName("test") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext

file_df = spark.read.csv("data/origin_news_data.csv", sep=',', header=True, encoding='GBK')

# file_list是一个[Row(title='xxx', main_body='xxx', link='xxx'), ..., Row()]
file_list = file_df.collect()
# print(file_list)

# set the cpu core number in the second parameter.
data_rdd = sc.parallelize(file_list, 2)
# type(data_rdd) = <class 'pyspark.rdd.RDD'>

# segmentate title into key words, parameter is a row of RDD
def word_seg(single_news_piece):
    title_string = str(single_news_piece['title'])
    # extract key words, topK is the K biggest values of TF-IDF index.
    title_seg = analyse.extract_tags(title_string, topK=10, withWeight=False, allowPOS=())
    return Row(title = str(single_news_piece['main_body']),
               main_body = str(single_news_piece['main_body']),
               link = str(single_news_piece['link']),
               title_seg = title_seg)

word_seg_rdd = data_rdd.map(word_seg)
# # create dataframe from rdd
# 对word_seg_df = SQLContext(sc).createDataFrame(word_seg_rdd)



print(word_seg_rdd.collect())




