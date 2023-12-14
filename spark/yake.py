
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
import yake

# Create a Spark session
spark = SparkSession.builder.master("local").appName("YAKEKeywordExtraction").getOrCreate()

# Define the YAKE keyword extraction function
def get_keywords_yake(text):
    y = yake.KeywordExtractor(lan='en',
                             n=3,
                             dedupLim=0.73,
                             dedupFunc='seqm',
                             windowsSize=1,
                             top=20,
                             features=None)

    keywords = y.extract_keywords(text)
    return keywords

# Register the UDF (User Defined Function) to use in Spark DataFrame
schema = StructType([StructField("title", StringType(), True),
                     StructField("abstract", StringType(), True)])

get_keywords_udf = udf(get_keywords_yake, ArrayType(StructType([
    StructField("keyword", StringType(), True),
    StructField("score", StringType(), True)
])))

# Read data from CSV file
csv_path = "/user/maria_dev/arxiv-2023.csv"
df = spark.read.csv(csv_path, header=True, schema=schema)

# Apply the YAKE UDF to the DataFrame
result_df = df.withColumn("keywords", get_keywords_udf("abstract"))

# Show the result
result_df.select("title", "keywords").show(truncate=False)

# Stop the Spark session
spark.stop()

