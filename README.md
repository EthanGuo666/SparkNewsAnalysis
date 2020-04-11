# SparkNewsAnalysis
🎓BITCS degree thesis project-Mass News Hotspot Analysis System Based On Spark.

## 1. Data Collecting:

- Run GetLinks.py to fetch the url links of news pages from the website of the China Association for Science and Technology.
- Run GetContents.py to download news title, main body and write into news_data.csv file.
<br/>(*There are still bugs in GetContents.py, but I'm not gonna refine it.)
## 2. Data preprocessing:
Run Preprocessing.py:
### 1. News Text Classification
- Function classifying() is to roughly classify the data into three categories.
### 2. Chinese Word Segmentation
- Function word_segmentation()
