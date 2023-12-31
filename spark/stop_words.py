from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer, Tokenizer
from pyspark.sql.types import StructType, StringType
import re
import os
from urllib.parse import urlparse

def get_all_csv_path(spark, path):
	hadoop = spark._jvm.org.apache.hadoop
	fs = hadoop.fs.FileSystem
	conf = hadoop.conf.Configuration()

	all_csv_files = []
	input_path = hadoop.fs.Path(path)
	csv_paths = [str(file.getPath()) for file in fs.get(conf).listStatus(input_path)]

	for csv_path in csv_paths:
		if csv_path.endswith(".csv"):
			csv_file_dir = "hdfs:///" + str(urlparse(csv_path).path)
			all_csv_files.append(csv_file_dir)

	return all_csv_files

def paper_processing(spark, df):
    df = df.withColumn('Title', F.regexp_replace('Title', 'Title: ', ''))
    #df = df.withColumn('Authors', F.regexp_replace('Authors', 'Authors: ', ''))
    #df = df.withColumn('Subjects', F.regexp_replace('Subjects', 'Subjects: ', ''))
    df.show()
    
    return df

if __name__ == "__main__":
	spark = SparkSession.builder.appName("PaperProcessing").getOrCreate()

	path = '/user/maria_dev/archive_store/raw/'
	csv_paths = get_all_csv_path(spark, path)
	print("get_path")
	print(csv_paths)

	for csv_path in csv_paths:
		check_csv = spark.read.option("header", "true")\
				.option("multiLine", "true")\
				.option('escape', ',')\
				.option('escape', '"')\
				.option("delimiter", ",")\
				.csv(csv_path)

        if check_csv.count() > 0:
            df = check_csv
            processed_df = paper_processing(spark, df)
            processed_df = processed_df.withColumn("Title", F.lower(processed_df.Title))
            processed_df = processed_df.withColumn("Abstract", F.lower(F.col('Abstract')))

			# Specifying a Language for StopWords
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
			df_new = df3.select('Year', 'Month', 'Authors', 'Subjects', 'new_sw_Title', 'new_sw_Abstract')

			# Extraction of the Paper Year
			paper_year = df_new.select("Year").distinct().collect()
			print(paper_year)
			
			for row in paper_year:
				save_df = df_new.where(F.col("Year") == row.Year)
				save_df.show(10)
				save_dir = "hdfs:///user/maria_dev/archive_store/stop_word/Stop_Word_" + row.Year
				print(save_dir)
				save_df.write.option("header", "true").csv(save_dir)
				print("SAVE: " + row.Year)
