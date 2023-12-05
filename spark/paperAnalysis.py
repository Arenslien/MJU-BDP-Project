from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if __name__=="__main__":
	
	# Creating SparkSession
	spark = SparkSession.builder.appName("Yearly Paper Analysis").getOrCreate()
	
	# 1. Load Data
	cs_2021_full_df = spark.read.option("header", "true").option("multiLine", "true").csv("hdfs:///user/maria_dev/arxiv-data/arxiv_CS_2021_full.csv")
	# cs_2021_full_df = cs_2021_full_df.limit(100)

	# View Data
	cs_2021_full_df.show(5)

	# 2. Yearly(+ Monthly) Abstract Keyword Analysis
	# 2.1 Split Abstract & Word Count
	word_count_df = cs_2021_full_df.withColumn("Words", F.split(F.col("Abstract\r"), " ")) \
			.select("Month", F.explode("Words").alias("Word")) \
			.groupBy("Month", "Word") \
			.count() \
			.cache()
	word_count_df.show(5)
	
	# 2.2 Sorting by Count --> Desc
	window_spec = Window.partitionBy("Month").orderBy(F.col("count").desc())
	windowed_word_count_df = word_count_df.withColumn("row_num", F.row_number().over(window_spec))

	# 2.3 Extract top K words
	K=30
	top_K_per_month = windowed_word_count_df.filter(F.col("row_num") <= K)

	# 2.4 Print Result
	top_K_per_month.show()
	


	# 3. Yearly(+ Monthly) Title Keyword Analysis


	# 4. Yearly(+ Monthly) Paper Publication Count


	
	
