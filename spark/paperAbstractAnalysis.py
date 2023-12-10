from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, desc, row_number
from pyspark.sql.window import Window
from urllib.parse import urlparse
import os

def get_all_path(spark, path):
	
	hadoop = spark._jvm.org.apache.hadoop
	fs = hadoop.fs.FileSystem
	conf = hadoop.conf.Configuration()
	all_paths = hadoop.fs.Path(path)
	dir_path = [str(f.getPath()) for f in fs.get(conf).listStatus(all_paths)]
	
	parsed_path = ["hdfs://" + str(urlparse(f).path) + "/*/*.csv" for f in dir_path]
	
	return parsed_path


if __name__=="__main__":

	# Creating SparkSession
	spark = SparkSession.builder.appName("Yearly Paper Analysis").getOrCreate()
	
	# 0. directory setting
	path = "hdfs:///user/maria_dev/arxiv-data/2015/"
	get_path = get_all_path(spark, path)
	print(get_path)
	
	#for dir in get_path:
	
	# 1. Load Data	
	cs_2021_full_df = spark.read.option("header", "true") \
			.option("multiLine", "true") \
			.option("escape", ",") \
			.option("escape", '"') \
			.csv("hdfs:///user/maria_dev/arxiv-data/2021/arxiv_CS_2021_full.csv")
	cs_2021_full_df = cs_2021_full_df.limit(30)

	# 2. Yearly(+ Monthly) Abstract Keyword Analysis
	
	# 2.1 Split Abstract & Word Count
	splited_df = cs_2021_full_df.withColumn("Words", split(col("Abstract\r"), " ")) \
			.select("Month", explode("Words").alias("Word"))
	
	# 2.2 Word Count
	word_count_df = splited_df.groupBy("Month", "Word") \
			.count()
	
	# 2.3 Sorting  Count --> Desc
	sorted_df = word_count_df.orderBy("Month", desc("count"))
	sorted_df.show()
	
	# 2.4 Saving top K keyword per Month 
	months = sorted_df.select("Month").distinct().collect()
	
	K = 30
	for row in months:
		top_K_keyword = sorted_df.where(col("Month") == row.Month).limit(30)
		top_K_keyword.show(10)
		save_dir = "hdfs:///user/maria_dev/arxiv-analysis-result/2021/abstract-keyword-" + row.Month
		top_K_keyword.write.csv(save_dir)
	

