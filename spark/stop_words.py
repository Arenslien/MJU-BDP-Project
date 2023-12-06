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
    processed_df = processed_df.withColumn("Title", F.lower(processed_df.Title))
    processed_df = processed_df.withColumn("Abstract\r", F.lower(F.col('Abstract\r')))
    #processed_df.show()
    #print(processed_df.columns)

    eng_stopwords = StopWordsRemover.loadDefaultStopWords("english")

    tk = RegexTokenizer(pattern=r'(?:\p{Punct}|\s)+', inputCol="Title", outputCol='tk_Title')

    df1 = tk.transform(processed_df)
    df1 = tk.setParams(inputCol="Abstract\r", outputCol="tk_Abstract").transform(df1)
    #df1.show()

    sw = StopWordsRemover(inputCol='tk_Title', outputCol='sw_Title', stopWords=eng_stopwords)
    df2 = sw.transform(df1)
    df2 = sw.setInputCol('tk_Abstract').setOutputCol("sw_Abstract").transform(df2)
#    df2.show()

    df3 = df2.withColumn("new_sw_Title", F.concat_ws(" ", "sw_Title"))
    df3 = df3.withColumn("new_sw_Abstract", F.concat_ws(" ", "sw_Abstract"))
#    df3.select("new_tk_Title", "new_tk_Abstract").show()

    df_new = df3.select('Month','Year', 'Subjects', 'new_sw_Title', 'new_sw_Abstract')

    #df_new.show()

    df_new.write.csv("hdfs:///user/maria_dev/arxiv-analysis-result/2021/StopWords_2021.csv")