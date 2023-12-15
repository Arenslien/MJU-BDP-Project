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
                           .csv("hdfs:///user/maria_dev/archive_store/2023/part-00000-8f0e6850-4de5-4a06-8dea-974d9aca6692-c000.csv", inferSchema=True)

    df.show()
    df.printSchema()
    print(df.count())
