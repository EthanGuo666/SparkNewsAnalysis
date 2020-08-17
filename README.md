# 🎓SparkNewsAnalysis
SparkNewsAnalysis is a degree thesis project finished in my last undergraduate year at BITCS.

<h1 align="center">
  <img src="https://github.com/EasonGuo666/SparkNewsAnalysis/blob/master/data/wordcloud.png" alt="wordcloud" width="600">
</h1>

## 👉Introduction
This amazing project can analyze tremendous amount of news data and rank them in order of topic heat.
<br/>The input is a csv file storing news piece and the output is rankings in dataframe and wordcloud picture.

## 🎯How it works

Three steps in total.

### 1. Data Collecting:

- Run `GetLinks.py` to fetch the url links of news pages from the website of the China Association for Science and Technology.
- Run `GetContents.py` to download news title, main body and write into `news_data.csv` file.

~~Of course there are still bugs in step1, but I just don't wanna refine it.~~

### 2. Data processing:
Run `Processing.py`, there are five substeps in this part.

##### 1. Import needed python libraries and start spark.
- Too easy, just skip~

##### 2. Read the csv data file and stopwords text file.
- Run function `spark.read.csv()` and `pd.read_csv()`.

##### 3. Segmentate Chinese words, remove all the stop words, blank space, and words whose length=1.
- Run function `tokenizer()`.

##### 4. News Text Classification (core step!)
- Run function `Word2Vec()` to create a wordvector object.
- Transfer words list into high dimension vectors.
- Calculate cosine similarity between category center and vectors of corresponding pieces of news data, classify the news into the category with highest similarity.

### 3. Statistics and visualization

This step contains three features:
- ① TopN titles in a category;
- ② TopN words in a category;
- ③ Location distribution.

Which are easy to understand.
