from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from functools import reduce
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer, Tokenizer

def paper_processing(spark, df):
    df = df.withColumn('Title', F.regexp_replace('Title', 'Title: ', ''))
    df = df.withColumn('Authors', F.regexp_replace('Authors', 'Authors: ', ''))
    df = df.withColumn('Subjects', F.regexp_replace('Subjects', 'Subjects: ', ''))

    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PaperProcessing").getOrCreate()
    cs_2021_full_df = spark.read.option("header", "true")\
                                .option("multiLine", "true")\
                                .option('escape', ',')\
                                .option('escape', '"')\
                                .csv("hdfs:///user/maria_dev/arxiv-data/arxiv_CS_2021_full.csv")
    #cs_2021_full_df.show()

    processed_df = paper_processing(spark, cs_2021_full_df)

    eng_stopwords = StopWordsRemover.loadDefaultStopWords("english")

    tk = RegexTokenizer(pattern=r'(?:\p{Punct}|\s)+', inputCol='Title', outputCol='tk_Title')
    df1 = tk.transform(processed_df).select("tk_Title")

    sw = StopWordsRemover(inputCol='tk_Title', outputCol='sw_Title', stopWords=eng_stopwords)
    df2 = sw.transform(df1)
    df2.show()
