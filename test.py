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


