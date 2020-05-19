# SparkNewsAnalysis
🎓BITCS degree thesis project-Mass News Hotspot Analysis System Based On Spark.

## 1. Data Collecting:

- Run GetLinks.py to fetch the url links of news pages from the website of the China Association for Science and Technology.
- Run GetContents.py to download news title, main body and write into news_data.csv file.
<br/>(*There are still bugs in GetContents.py, but I'm not gonna refine it.)
## 2. Data processing:
Run Processing.py, there are five substeps in this part.
#### 1. Import needed python libraries and start spark.
- Too easy, just skip~
#### 2. Read the csv data file and stopwords text file.
- Run function spark.read.csv() and pd.read_csv().
#### 3. Segmentate Chinese words, remove all the stop words, blank space, and words whose length=1.
- Run function tokenizer().
#### 4. News Text Classification (core step!)
- Run function Word2Vec() to create a wordvector object.
- Transfer words list into high dimension vectors.
- Calculate cosine similarity between category center and vectors of corresponding pieces of news data, classify the news into the category with highest similarity.
#### 5. Statistics and visualization
- this step