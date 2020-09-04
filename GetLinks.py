import numpy as np
import requests
import pprint
import json
import re
from bs4 import BeautifulSoup

#下载html文件下所有资源
def download_html(url):
    htmls = []
    print("url is:", url)
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception("error")
    text = str(r.content, "utf-8")
    htmls.append(text)
    return htmls[0]

#抓取单个网页下的10个链接
def get_links(html):
    soup = BeautifulSoup(html, 'html.parser')
    print(html, "\n************************\n")
    # 先解析出含有标题区域的html代码
    #titles_code = soup.find_all("script", type="text/xml")
    titles_code = soup.find_all("div", id = "78504")
    # 目标字符串是 /art/2020/3/28/art_80_117514.html 这种形式
    pattern = re.compile(r'href=\"/art/\d*/\d*/\d*/art_\d*_\d*.html')
    matches = pattern.findall(str(titles_code[0]))
    # 去除重复
    matches = list(set(matches))
    #print(matches)
    links = []
    prefix = "http://www.bast.net.cn"
    for each in matches:
        full_link = prefix + each[6:]
        r = requests.get(full_link)
        if r.status_code != 200:
            print("error link:", full_link)
        else:
            print("get link:", full_link)
        links.append(full_link)
    print(links)
    return links
 
global target_links
target_links = []

target_url = "http://www.bast.net.cn/col/col23312/index.html"
target_html = download_html(target_url)
target_links += get_links(target_html)
