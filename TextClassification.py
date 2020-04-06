import pandas as pd

# Read data
path = "data/origin_news_data.csv"
data = pd.read_csv(path, encoding = 'GBK')

# index是表格左边那一排纵向的0，到100...
# 横行数量                  # 纵列数量                   # 取表格的某整列
#print(data.shape[0])      #print(data.shape[1])       # #print(data.link)

# 中国科协
national_assoc = 0
# 地方科协
local_assoc = 1
# 全国学会
national_academy = 2

# 互斥量，每条新闻只能改一次属性
mutex = 0
def addtype():
    # 遍历整张表, 也可以用
    # for row in data.itertuples(index=True, name='Pandas'):
    #     print(getattr(row, "title"))
    for index, row in data.iterrows():
        # print("data.iloc[index].type:",data.iloc[index].type)
        text = data.iloc[index].title + " " + data.iloc[index].main_body
        if "中国科协" in text:
            data.iloc[index].type = 0
            print("8\n8\n8\n8\n8\n8\n8\n")
        if ("省科协" in text) or ("市科协" in row.title):
            data.iloc[index].type = 1
        if "学会" in text:
            data.iloc[index].type = 2
        print("data.iloc[index]:", data.iloc[index])
addtype()