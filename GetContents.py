import requests
import string
#from GetLinks import target_links
from bs4 import BeautifulSoup

links_file = open("bj_links.txt","r")
target_links = links_file.readlines()

news_list = []
for link in target_links:
    print("current link is:", link[:-1])
    r = requests.get(link)
    r.encoding = 'utf-8'
    html = r.text
    soup1 = BeautifulSoup(html, "html.parser")

    # 获取标题
    title_html = soup1.find("div", {"class":"arctit"})
    print(title_html)


#     # 获取正文
#     paragraphs_html = soup1.find_all("p", style=None)
#     paragraphs = ""
#     for i in paragraphs_html:
#         if not i.find("span"):
#             temp = str(i)
#             paragraphs = paragraphs + temp[3:-4]
#
#     max_num = 1000
#     #去除\u3000码
#     main_body = paragraphs.replace("\u3000", " ", max_num)\
#                             .replace("\xa0", " ", max_num)\
#                             .replace("<br/>", "", max_num)\
#                             .replace("<strong>", "", max_num)\
#                             .replace("</strong>", "", max_num)
#
#     #
#     if len(paragraphs) > 50:
#         #print(paragraphs)
#         news_piece = {"title":title, "main_body":main_body, "link":link}
#         print(news_piece)
#         news_list.append(news_piece)
#
# import os
# import csv
# import pandas as pd
#
# file_path = 'c:\\Users\\lenovo\\Desktop\\news.csv'
#
# '''
# with open(file_path, mode="w", newline='') as t_file:
#     csv_writer = csv.writer(t_file)
#     for i in news_list:
#         #print(i)
#         #print("######")
#         csv_writer.writerow(i["title"])
# '''
#
# with open(file_path, 'w', encoding='utf-8-sig', newline='') as csvfile:
#     fieldnames = ['title', 'main_body', 'link']
#     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#
#     writer.writeheader()
#     for i in news_list:
#         writer.writerow(i)
#
# print("writing complete")