##################################################################
# 以下为pandas版本的正确代码
# import pandas as pd
#
# # Read data
# path = "data/origin_news_data.csv"
# data = pd.read_csv(path, encoding='GBK')
#
# def classifying():
#     # 中国科协
#     national_assoc_data = pd.DataFrame()
#     # 地方科协
#     local_assoc_data = pd.DataFrame()
#     # 全国学会
#     national_academy_data = pd.DataFrame()
#     # 遍历整张表
#     for i in range(len(data)):
#         text = str(data.loc[[i], ['title']]) + " " + str(data.loc[[i], ['main_body']])
#         if ("省科" in text) or ("市科" in text) or ("省政府" in text) or ("市政府" in text) or ("基层" in text) or ("区" in text):
#             local_assoc_data = local_assoc_data.append(data.loc[[i]], ignore_index = True)
#         elif "学会" in text:
#             national_academy_data = national_academy_data.append(data.loc[[i]], ignore_index = True)
#         else:
#             national_assoc_data = national_assoc_data.append(data.loc[[i]], ignore_index = True)
#     local_assoc_data.to_csv("data/local_assoc_data.csv")
#     national_academy_data.to_csv("data/national_academy_data.csv")
#     national_assoc_data.to_csv("data/national_assoc_data.csv")
#
# classifying()
################################################################################################

# 以下spark版本的文本粗分类
from pyspark.sql import SparkSession

# 启动spark
spark = SparkSession.builder.master("local") \
    .appName("test") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext

# 读入csv
fileDF = spark.read.csv("data/origin_news_data.csv", sep=',', header=True, encoding='GBK')

global local_assoc_data , national_academy_data, national_assoc_data

def classifying():
    # 地方科协
    local1 = fileDF.filter(fileDF["title"].contains("省科"))
    local2 = fileDF.filter(fileDF['title'].contains("市科"))
    local3 = fileDF.filter(fileDF['title'].contains("省政"))
    local4 = fileDF.filter(fileDF['title'].contains("市政"))
    local5 = fileDF.filter(fileDF['title'].contains("区"))
    local6 = fileDF.filter(fileDF['title'].contains("基层"))
    #求并集，去重
    local_assoc_data1 = local1.union(local2).union(local3).union(local4).union(local5).union(local6).distinct()
    # √ local_assoc_data.write.csv("data/local_assoc_data.csv", "overwrite", encoding='GBK')

    # 全国学会
    national_academy_data1 = fileDF.filter(fileDF["title"].contains("学会")).distinct()
    # √ national_academy_data.write.csv("data/national_academy_data.csv", "overwrite", encoding='GBK')

    # 中国科协
    national_assoc_data1 = fileDF.filter(fileDF["title"].contains("中国科协")).distinct()
    # √ national_assoc_data.write.csv("data/national_assoc_data.csv", "overwrite", encoding='GBK')
    return local_assoc_data1, national_academy_data1, national_assoc_data1

# Execute the classification.
local_assoc_data, national_academy_data, national_assoc_data = classifying()

