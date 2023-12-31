
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer, Tokenizer
from pyspark.sql.types import StructType, StringType
import re
import os
from urllib.parse import urlparse

def get_all_path(spark, path):
    hadoop = spark._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()

    all_paths = hadoop.fs.Path(path)
    dir_path = [str(f.getPath()) for f in fs.get(conf).listStatus(all_paths)]

    parsed_path = ["hdfs://"+str(urlparse(f).path)+"/*/*.csv" for f in dir_path]

    return parsed_path

def paper_processing(spark, df):
    df = df.withColumn('Title', F.regexp_replace('Title', 'Title: ', ''))
    #df = df.withColumn('Authors', F.regexp_replace('Authors', 'Authors: ', ''))
    #df = df.withColumn('Subjects', F.regexp_replace('Subjects', 'Subjects: ', ''))

    return df


if __name__ == "__main__":
    spark = SparkSession.builder.appName("PaperProcessing").getOrCreate()

    df = spark.read.option("header", "true")\
                           .option("multiLine", "true")\
                           .option('escape', ',')\
                           .option('escape', '"')\
                           .csv("hdfs:///user/maria_dev/archive_store/2023/part-00000-8f0e6850-4de5-4a06-8dea-974d9aca6692-c000.csv", inferSchema=True)\
                           .select("Year", "Month", "Title", "Abstract")

    processed_df = paper_processing(spark, df)
    processed_df = processed_df.withColumn("Title", F.lower(processed_df.Title))
    processed_df = processed_df.withColumn("Abstract", F.lower(F.col('Abstract')))

    eng_stopwords = StopWordsRemover.loadDefaultStopWords("english")

    tk = RegexTokenizer(pattern=r'(?:\p{Punct}|\s)+', inputCol="Title", outputCol='tk_Title')

    df1 = tk.transform(processed_df)
    df1 = tk.setParams(inputCol="Abstract", outputCol="tk_Abstract").transform(df1)

    # Removing StopWords from the 'Title' column
    sw = StopWordsRemover(inputCol='tk_Title', outputCol='sw_Title', stopWords=eng_stopwords)
    df2 = sw.transform(df1)
    # Removing StopWords from the 'Abstract' column
    df2 = sw.setInputCol('tk_Abstract').setOutputCol("sw_Abstract").transform(df2)


    # Convert array<string> type to string type
    df3 = df2.withColumn("new_sw_Title", F.concat_ws(" ", "sw_Title"))
    df3 = df3.withColumn("new_sw_Abstract", F.concat_ws(" ", "sw_Abstract"))

    # Selection
    df_new = df3.select('Year', 'Month', 'new_sw_Title', 'new_sw_Abstract')
    df_new.show()

    df.printSchema()
    print(df.count())

    # Save file
    df_new.write.option("header",True)\
                .csv(f"hdfs:///user/maria_dev/arxiv-analysis-result/2033/StopWords_2033/")

