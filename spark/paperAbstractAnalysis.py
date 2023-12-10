from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, desc, row_number
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
from urllib.parse import urlparse
import os

def get_all_path(spark, path):
	
	hadoop = spark._jvm.org.apache.hadoop
	fs = hadoop.fs.FileSystem
	conf = hadoop.conf.Configuration()
	input_path = hadoop.fs.Path(path)
	year_paths = [str(f.getPath()) for f in fs.get(conf).listStatus(input_path)]
	csv_paths = []
	
	for y_path in year_paths:
		year_path = hadoop.fs.Path(y_path)
		file_paths = [str(file.getPath()) for file in fs.get(conf).listStatus(year_path)]

		for f_path in file_paths:
			if f_path.endswith(".csv"):
				csv_path = "hdfs://" + str(urlparse(f_path).path)
				csv_paths.append(csv_path)

	return csv_paths

if __name__=="__main__":

	# Creating SparkSession
	spark = SparkSession.builder.appName("Yearly Paper Analysis").getOrCreate()
	
	# 0. directory setting
	path = "hdfs:///user/maria_dev/archive_store/stop_word/"
	csv_paths = get_all_path(spark, path)
	print(csv_paths)
	
	all_years = []
	all_year_publication = []
	
	publicationCountSchema = StructType([
		StructField("Year", StringType(), nullable=False),
		StructField("count", LongType(), nullable=False)
	])
	publication_count_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=publicationCountSchema)

	for csv_path in csv_paths:
		# 1. Load Data	
		csv_df = spark.read.option("header", "true") \
				.option("multiLine", "true") \
				.option("escape", ",") \
				.option("escape", '"') \
				.csv(csv_path)
		csv_df = csv_df.limit(30)

		# 2. Add Yearly Publication Counting
		grouped_df = csv_df.groupBy("Year").count()
		publication_count_df = publication_count_df.union(grouped_df)

		# 3. Yearly(+ Monthly) Abstract Keyword Analysis
		
		# 3.1 Split Abstract & Word Count
		splited_df = csv_df.withColumn("Words", split(col("Abstract"), " ")) \
				.select("Year", explode("Words").alias("Word"))
		
		# 3.2 Word Count
		word_count_df = splited_df.groupBy("Year", "Word") \
			.count()
	
		# 3.3 Sorting  Count --> Desc
		sorted_df = word_count_df.orderBy("Year", desc("count"))
		sorted_df.show()
	
		# 3.4 Saving top K keyword per Month 
		years = sorted_df.select("Year").distinct().collect()
	
		K = 30
		for row in years:
			top_K_keyword = sorted_df.where(col("Year") == row.Year).limit(30)
			top_K_keyword.show(10)
			save_dir = "hdfs:///user/maria_dev/archive_store/abstract-keyword-" + row.Year
			top_K_keyword.write.csv(save_dir)
	
	# 2.1 Save Yearly Publication Count
	save_dir2 = "hdfs:///user/maria_dev/archive_store/Yearly-Publication-Count"
	publication_count_df.write.csv(save_dir2)


