import jieba
import numpy as np
import pandas as pd
import PIL.Image as image
from wordcloud import WordCloud
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.types import StringType
from pyspark.ml.feature import Word2Vec
from pyspark.ml import Pipeline
from pyspark.ml.linalg import DenseVector

# 启动spark
spark = SparkSession.builder.master("local") \
    .appName("Preprocessing") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext

cpu_cores = 3
word_vector_size = 300

file_df = spark.read.csv("data/real_data.csv", sep=',', header=True, encoding='GBK')
# file_list是一个[Row(title='xxx', main_body='xxx', link='xxx'), Row(), ... , Row()]
file_list = file_df.collect()
# print(file_list)

# set the cpu core number in the second parameter.
data_rdd = sc.parallelize(file_list, cpu_cores)
# type(data_rdd) = <class 'pyspark.rdd.RDD'>

stopwords = pd.read_csv("data/stopwords.txt", index_col=False, quoting=3,
                        sep="\t", names=['stopword'], encoding='utf-8')
stopwords = stopwords['stopword'].values


# segmentate title into key words, parameter is a row of RDD
def tokenizer(single_news_piece):
    # title_string = str(single_news_piece['title'])
    title_string = single_news_piece['title']
    # main_body_string = str(single_news_piece['main_body'])
    main_body_string = single_news_piece['main_body']
    cmb_string = title_string + main_body_string
    # extract key words, topK is the K biggest values of TF-IDF index.
    # words = analyse.extract_tags(cmb_string, topK=50, withWeight=False, allowPOS=())
    tmp_seg = jieba.cut(cmb_string)
    words = ','.join(tmp_seg)
    words_list = words.split(',')
    words_list = list(filter(lambda x: x not in stopwords, words_list))
    # 移除空格
    words = list(filter(lambda x: len(x) > 1, words_list))
    return Row(title=title_string,
               main_body=main_body_string,
               # link = str(single_news_piece['link']),
               link=single_news_piece['link'],
               words=words)


word_seg_rdd = data_rdd.map(tokenizer)
# # create dataframe from rdd
# 对word_seg_df = SQLContext(sc).createDataFrame(word_seg_rdd)
# 对word_seg_df = word_seg_rdd.toDF()

# print(type(word_seg_rdd.collect()))

# word_seg_df = SQLContext(sc).createDataFrame(word_seg_rdd)
word_seg_df = word_seg_rdd.toDF()
word_seg_df.show()

w2v = Word2Vec(vectorSize=word_vector_size, minCount=0, inputCol="words", outputCol="features")
doc2vec_pipeline = Pipeline(stages=[w2v])
doc2vec_model = doc2vec_pipeline.fit(word_seg_df)
doc2vecs_df = doc2vec_model.transform(word_seg_df)
# doc2vecs_df.select('title','features').show(doc2vecs_df.count())

featured_data_list = doc2vecs_df.collect()
featured_data_rdd = sc.parallelize(featured_data_list, cpu_cores)
print(featured_data_rdd.collect())

category_center0 = featured_data_list[0]['features']
category_center1 = featured_data_list[1]['features']
category_center2 = featured_data_list[2]['features']
category_center3 = featured_data_list[3]['features']
category_center4 = featured_data_list[4]['features']
category_center5 = featured_data_list[5]['features']
category_center6 = featured_data_list[6]['features']
category_center7 = featured_data_list[7]['features']


# print(type(category_center0)) = <class 'pyspark.ml.linalg.DenseVector'>

def cosine_value(vector1, vector2):
    return vector1.dot(vector2) / (vector1.norm(2) * vector2.norm(2))


# calculate cosine similarity of news data and category centers.
def add_cosine_similarity(single_line_rdd):
    data_vector = single_line_rdd['features']
    distance = list()
    distance.append(cosine_value(data_vector, category_center0))
    distance.append(cosine_value(data_vector, category_center1))
    distance.append(cosine_value(data_vector, category_center2))
    distance.append(cosine_value(data_vector, category_center3))
    distance.append(cosine_value(data_vector, category_center4))
    distance.append(cosine_value(data_vector, category_center5))
    distance.append(cosine_value(data_vector, category_center6))
    distance.append(cosine_value(data_vector, category_center7))
    max_value = max(distance)
    category = distance.index(max_value)
    return Row(title=single_line_rdd['title'],
               main_body=single_line_rdd['main_body'],
               link=single_line_rdd['link'],
               words=single_line_rdd['words'],
               features=single_line_rdd['features'],
               cos_similarity=float(max_value),
               category=category)

classified_data = featured_data_rdd.map(add_cosine_similarity)
classified_df = classified_data.toDF()
classified_df.show()

def compare_distance(num):
    print(featured_data_rdd.collect()[num]['title'])
    v = featured_data_rdd.collect()[num]['features']
    d0 = cosine_value(v, category_center0)
    print(d0)
    d1 = cosine_value(v, category_center1)
    print(d1)
    d2 = cosine_value(v, category_center2)
    print(d2)
    d3 = cosine_value(v, category_center3)
    print(d3)
    d4 = cosine_value(v, category_center4)
    print(d4)
    d5 = cosine_value(v, category_center5)
    print(d5)
    d6 = cosine_value(v, category_center6)
    print(d6)
    d7 = cosine_value(v, category_center7)
    print(d7)

# compare_distance(8)

classfication = list()
classfication.append(classified_df.where(classified_df.category == 0))
classfication.append(classified_df.where(classified_df.category == 1))
classfication.append(classified_df.where(classified_df.category == 2))
classfication.append(classified_df.where(classified_df.category == 3))
classfication.append(classified_df.where(classified_df.category == 4))
classfication.append(classified_df.where(classified_df.category == 5))
classfication.append(classified_df.where(classified_df.category == 6))
classfication.append(classified_df.where(classified_df.category == 7))
for i in classfication:
    print(i.count())

# 1.统计topN标题
# 统计每个类别下的和类别最接近的topN个标题和对应URL。

print(type(classfication[3]))

def topN_sim_in_category(class_num):
    sorted_class = classfication[class_num].orderBy(classfication[class_num].cos_similarity.desc())
    sorted_class.select('cos_similarity', 'title', 'link').show()

for i in range(8):
    topN_sim_in_category(i)


# 2.统计词语频率
# 统计每个类别下的词语频率，并按照词频从高到低排序，取TopN个高频词。

# 词频计数
def word_count(listname):
    wordRDD = sc.parallelize(listname)
    wordCountRDD = (wordRDD
                    .map(lambda x: (x, 1))
                    .reduceByKey(lambda x, y: x + y))
    return wordCountRDD.collect()


# 词频排序
def topN_word_in_category(class_num):
    row_list = classfication[class_num].select('words').collect()
    word_list = list()
    for i in row_list:
        word_list += i['words']
    # print(word_list)
    word_frequency_list = word_count(word_list)
    # 按词频降序排列
    word_frequency_list.sort(key=lambda x: x[1], reverse=True)
    return word_frequency_list

word_freq_list = topN_word_in_category(3)


# 根据词频生成词云
def generate_wordcloud(word_list):
    text = str()
    if len(word_list) > 150:
        for i in word_list[:150]:
            text += i[0] + ' '
    else:
        for i in word_list:
            text += i[0] + ' '
    # print(text)
    mask = np.array(image.open("data/China.png"))
    # 添加遮罩层, 生成中文字的字体
    wordcloud = WordCloud(mask=mask,
                          font_path="C:\Windows\Fonts\Deng.ttf",
                          background_color='white').generate_from_text(text)
    image_produce = wordcloud.to_image()
    image_produce.show()

generate_wordcloud(word_freq_list)


# 3.统计地域分布
# 统计每个类别下的地域分布，包括省份、城市和国家。

def loc_distribution(class_num):
    province = ['北京', '上海', '天津', '重庆', '新疆', '西藏', '青海', '甘肃', '宁夏', '内蒙',
                '黑龙江', '吉林', '辽宁', '河北', '河南', '陕西', '山西', '山东', '湖北', '湖南',
                '安徽', '江苏', '浙江', '江西', '福建', '云南', '四川', '贵州', '海南', '广东',
                '广西', '香港', '澳门', '台湾']
    count = [0] * 34
    word_list = topN_word_in_category(class_num)
    for i in word_list:
        for j in province:
            if j in i[0]:
                count[province.index(j)] += i[1]
    distribution = list()
    for i in range(len(count)):
        distribution.append((province[i], count[i]))
    distribution.sort(key=lambda x: x[1], reverse=True)
    return distribution

loc_distribution(3)

# 4.统计站点分布
# 统计每个类别下的采集站点分布，站点频率从高到低排序，取TopN个站点。


# 5.热点话题分布
# 计算每个类别下的热点话题分布。
