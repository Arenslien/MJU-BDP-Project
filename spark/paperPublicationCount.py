from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, desc, row_number
from pyspark.sql.window import Window
import os

# def get_all_folders(path):
# 	hdfs = spark._jsparkSession.sparkContext._gateway \

if __name__=="__main__":
	
	# Creating SparkSession
	spark = SparkSession.builder.appName("Yearly Paper Analysis").getOrCreate()
	
	# 0. directory setting
	#arxiv_data_dir = "hdfs:///user/maria_dev/arxiv-data"
	#all_folders = get_all_folders(arxiv_data_dir)
	#print(all_folders)

	# 1. Load Data	
	cs_2021_full_df = spark.read.option("header", "true") \
			.option("multiLine", "true") \
			.option("escape", ",") \
			.option("escape", '"') \
			.csv("hdfs:///user/maria_dev/arxiv-data/2021/arxiv_CS_2021_full.csv")

    # 2. Yearly(+ Monthly) Paper Publication Count
	
	# 2.1 Monthly Paper Publication Counting
	splited_df = cs_2021_full_df.groupBy("Month", "Word") \
			.count()
	
	splited_df.show()
	
	# 2.2 Saving Counting per Month 
	months = sorted_df.select("Month").distinct().collect()
	
	for row in months:
		result = splited_df.where(col("Month") == row.Month)
		result.show()
		result.write.csv(f"hdfs:///user/maria_dev/arxiv-analysis-result/2021/{row.Month}MonthPublicationCounting.csv")
		