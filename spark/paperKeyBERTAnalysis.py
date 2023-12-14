from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, desc, udf
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
from urllib.parse import urlparse
import os
import nltk
import string
from keybert import KeyBERT
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

nltk.download('punkt')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger')

keyBERT_model = KeyBERT(model='all-mpnet-base-v2')

#불용어 리스트
stop_words = set(stopwords.words('english'))  # 언어에 따라 변경 가능

# 문장부호 문자열
punctuation = set(string.punctuation)

def get_all_csv_path(spark, path):
	
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

def generate_bigram(text):
	words = word_tokenize(text)
	filtered_words = [word.lower() for word in words if word.lower() not in stop_words and word not in punctuation]
	
	tagged_words = nltk.pos_tag(filtered_words)

	noun_words = [word for word, tag in tagged_words if tag in ["NN", "NNS", "NNP", "NNPS"]]

	bigrams = ["_".join(gram) for gram in ngrams(noun_words, 2)]

	return " ".join(bigrams)

def extract_keywords_KeyBERT(text):
	keywords = keyBERT_model.extract_keywords(text,
			keyphrase_ngram_range=(1, 3),
			stop_words='english',
			highlight=False,
			top_n=10
	)

	result = []
	keywords_list = list(dict(keywords).keys())
	
	for keywords in keywords_list:
		new_keywords = "_".join(keywords.split(" "))
		result.append(new_keywords)

	return " ".join(result)

if __name__=="__main__":

	# Creating SparkSession
	spark = SparkSession.builder.appName("Yearly Paper Analysis").getOrCreate()
	
	# 0. directory setting
	path = "hdfs:///user/maria_dev/archive_store/raw"
	csv_paths = get_all_csv_path(spark, path)
	print(csv_paths)
	
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
		csv_df.show(10)

		# 2. Add Yearly Publication Counting
		grouped_df = csv_df.groupBy("Year").count()
		publication_count_df = publication_count_df.union(grouped_df)

		# 3. Yearly Abstract Keyword Analysis --> KeyBERT
		keybert_udf = udf(extract_keywords_KeyBERT, StringType())
		keybert_csv_df = csv_df.withColumn("Abstract_KeyBERT", keybert_udf(col("Abstract"))) \
				.withColumn("Title_KeyBERT", keybert_udf(col("Title"))) # new_sw_Title
		
		bigram_udf = udf(generate_bigram, StringType())
		bigram_csv_df = csv_df.withColumn("Abstract_bigrams", bigram_udf(col("new_sw_Abstract")))\
				.withColumn("Title_bigrams", bigram_udf(col("new_sw_Title")))

		# 3.1 Split Abstract & Word Count
		splited_df = keybert_csv_df.withColumn("Words", split(col("Abstract_bigrams"), " ")) \
				.withColumn("Words_title", split(col("Title_bigrams"), " "))\
				.select("Year", explode("Words").alias("Word"))

		# 3.2 drop meaningless case 
		drop_words = ["e_g", "log_n", "et_al", "1_2", "n_1", "n_2", "n_log", "2_n", "paper_present", "paper_presents", "paper_propose", "results show", "state_art", "well_known", "real_world", "proposed_method"]
		splited_df = splited_df.filter(~col("Word").isin(*drop_words))
		
		# 3.3 Word Count
		word_count_df = splited_df.groupBy("Year", "Word") \
			.count()
	
		# 3.4 Sorting  Count --> Desc
		sorted_df = word_count_df.orderBy("Year", desc("count"))
		sorted_df.show()
	
		# 3.5 Saving top K keyword per Month 
		years = sorted_df.select("Year").distinct().collect()
	
		K = 30
		for row in years:
			top_K_keyword = sorted_df.where(col("Year") == row.Year).limit(K)
			top_K_keyword.show(K)
			save_dir = "hdfs:///user/maria_dev/archive_store/abstract-keyword-" + row.Year
			top_K_keyword.write.csv(save_dir)
	
	# 2.1 Save Yearly Publication Count
	save_dir2 = "hdfs:///user/maria_dev/archive_store/Yearly-Publication-Count"
	publication_count_df.write.csv(save_dir2)

