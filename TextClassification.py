import pandas as pd

# Read data
path = "data/origin_news_data.csv"
data = pd.read_csv(path, encoding='GBK')

# index是表格左边那一排纵向的0，到100...
# 横行数量                  # 纵列数量                   # 取表格的某整列
# print(data.shape[0])      #print(data.shape[1])       # #print(data.link)

# 中国科协
national_assoc = 0
# 地方科协
local_assoc = 1
# 全国学会
national_academy = 2

def addtype():
    # 遍历整张表
    for i in range(len(data)):
        text = str(data.loc[[i], ['title']]) + " " + str(data.loc[[i], ['main_body']])
        if ("省科" in text) or ("市科" in text) or ("省政府" in text) or ("市政府" in text) or ("基层" in text) or ("区" in text):
            data.loc[[i], ['type']] = local_assoc
        elif "学会" in text:
            data.loc[[i], ['type']] = national_academy
        else:
            data.loc[[i],['type']] = national_assoc
    # print(data)

addtype()
print(data)