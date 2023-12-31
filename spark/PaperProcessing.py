
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer
from pyspark.sql.types import StructType, StringType
import re
from urllib.parse import urlparse

def get_all_path(spark, path):
    hadoop = spark._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()

    # Get all paths
    all_paths = hadoop.fs.Path(path)
    dir_path = [str(f.getPath()) for f in fs.get(conf).listStatus(all_paths)]

    # Parse HDFS paths
    parsed_path = ["hdfs://"+str(urlparse(f).path)+"/part-00000-*-c000.csv" for f in dir_path]

    return parsed_path

def paper_processing(spark, df):
    # Remove unnecessary strings from 'Title', 'Authors', 'Subjects', 'Abstract'
    df = df.withColumn('Title', F.regexp_replace('Title', 'Title: ', ''))
#    df = df.withColumn('Authors', F.regexp_replace('Authors', 'Authors: ', ''))
 #   df = df.withColumn('Subjects', F.regexp_replace('Subjects', 'Subjects: ', ''))
    
    # Rename 'Abstract' column
    df = df.withColumn('Abstract', F.regexp_replace('Abstract\r', 'Abstract\r: ', ''))
    df.show()
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PaperProcessing").getOrCreate()

    path = '/user/maria_dev/archive_store/'
    get_path = get_all_path(spark, path)

    for dir in get_path:
        try:
            check_csv = spark.read.option("header", "true").csv(dir)
            if check_csv.count() > 0:
                df = check_csv
                processed_df = paper_processing(spark, df)
                processed_df = processed_df.withColumn("Title", F.lower(processed_df.Title))
                
                # Process newline characters in 'Abstract' column
                processed_df = processed_df.withColumn("Abstract", F.lower(F.regexp_replace('Abstract', '\r', ' ')))

                # Specify StopWords for the English language
                eng_stopwords = StopWordsRemover.loadDefaultStopWords("english")

                # Apply regex tokenization
                tk = RegexTokenizer(pattern=r'(?:\p{Punct}|\s)+', inputCol="Title", outputCol='tk_Title')
                df1 = tk.transform(processed_df)
                df1 = tk.setParams(inputCol="Abstract", outputCol="tk_Abstract").transform(df1)

                # Remove StopWords from the 'Title' column
                sw = StopWordsRemover(inputCol='tk_Title', outputCol='sw_Title', stopWords=eng_stopwords)
                df2 = sw.transform(df1)
                # Remove StopWords from the 'Abstract' column
                df2 = sw.setInputCol('tk_Abstract').setOutputCol("sw_Abstract").transform(df2)

                # Convert array<string> type to string type
                df3 = df2.withColumn("new_sw_Title", F.concat_ws(" ", "sw_Title"))
                df3 = df3.withColumn("new_sw_Abstract", F.concat_ws(" ", "sw_Abstract"))

                # Select relevant columns
                df_new = df3.select('Year', 'Month', 'Authors', 'Subjects', 'new_sw_Title', 'new_sw_Abstract')

                # Extract the paper year
                match = re.search(r'/(\d{4})/', dir)
                paper_year = str(match.group(1))

                # Save the processed file
                df_new.write.option("header", True).csv(f"hdfs:///user/maria_dev/arxiv-analysis-result/{paper_year}/StopWords_{paper_year}")

        except Exception as e:
            print(f"Error processing {dir}: {e}")

