
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer
import re
import os
from urllib.parse import urlparse

def get_all_path(spark, path):
    hadoop = spark._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()

    all_paths = hadoop.fs.Path(path)
    dir_path = [str(f.getPath()) for f in fs.get(conf).listStatus(all_paths)]

    parsed_path = ["hdfs://" + str(urlparse(f).path) + "/*/*.csv" for f in dir_path]

    return parsed_path

def paper_processing(spark, df):
    df = df.withColumn('Title', F.regexp_replace('Title', 'Title: ', ''))
    df = df.withColumn('Authors', F.regexp_replace('Authors', 'Authors: ', ''))
    df = df.withColumn('Subjects', F.regexp_replace('Subjects', 'Subjects: ', ''))

    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PaperProcessing").getOrCreate()

    path = '/user/maria_dev/archive_store/'
    get_path = get_all_path(spark, path)

    for dir in get_path:
        try:
            check_csv = spark.read.option("header", "true") \
                .option("multiLine", "true") \
                .option('escape', ',') \
                .option('escape', '"') \
                .option("mode", "DROPMALFORMED")\
                .csv(dir)
            if check_csv.count() > 0:
                df = check_csv
                processed_df = paper_processing(spark, df)
                processed_df = processed_df.withColumn("Title", F.lower(processed_df.Title))
                processed_df = processed_df.withColumn("Abstract", F.lower(F.col('Abstract')))

                eng_stopwords = StopWordsRemover.loadDefaultStopWords("english")

                tk = RegexTokenizer(pattern=r'(?:\p{Punct}|\s)+', inputCol="Title", outputCol='tk_Title')
                df1 = tk.transform(processed_df)
                df1 = tk.setParams(inputCol="Abstract", outputCol="tk_Abstract").transform(df1)

                sw = StopWordsRemover(inputCol='tk_Title', outputCol='sw_Title', stopWords=eng_stopwords)
                df2 = sw.transform(df1)
                df2 = sw.setInputCol('tk_Abstract').setOutputCol("sw_Abstract").transform(df2)

                df3 = df2.withColumn("new_sw_Title", F.concat_ws(" ", "sw_Title"))
                df3 = df3.withColumn("new_sw_Abstract", F.concat_ws(" ", "sw_Abstract"))

                df_new = df3.select('Year', 'Month', 'Authors', 'Subjects', 'new_sw_Title', 'new_sw_Abstract')

                match = re.search(r'/(\d{4})/', dir)
                paper_year = str(match.group(1))

                # Save file
                output_path = f"hdfs:///user/maria_dev/arxiv-analysis-result/{paper_year}/StopWords_{paper_year}/"

                # Create directory if it does not exist
                if not os.path.exists(output_path):
                    os.makedirs(output_path)

                df_new.write.option("header", True).format("csv").mode("overwrite").save(output_path)

        except Exception as e:
            print(f"Error processing {dir}: {str(e)}")
            pass

