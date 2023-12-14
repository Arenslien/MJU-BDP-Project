
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from bs4 import BeautifulSoup
import requests

def extract_paper_info(entry):
    title = entry.find("div", class_="list-title mathjax").text.strip()
    authors = [author.text.strip() for author in entry.find_all("a", href=lambda x: x and "/search/cs?searchtype=author" in x)]
    subjects = [subject.text.strip() for subject in entry.find_all("span", class_="primary-subject")]
    abstract = entry.find("p", class_="mathjax").text.strip()

    return title, authors, subjects, abstract

def fetch_page(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
        else:
            print(f"Failed to fetch page. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching page: {e}")
        return None

def crawl_papers(year, month, day):
    url = f"https://arxiv.org/catchup?syear={year}&smonth={month}&sday={day}&num=25&archive=cs&method=with"
    page_html = fetch_page(url)

    if page_html is not None:
        soup = BeautifulSoup(page_html, 'html.parser')
        papers = soup.find_all("dd")
        data = []

        for paper in papers:
            title, authors, subjects, abstract = extract_paper_info(paper)
            data.append([year, month, title, authors, subjects, abstract])

        return data
    else:
        return []

def main():
    spark = SparkSession.builder.appName("3").getOrCreate()

    # Update the schema to use ArrayType for Authors and Subjects
    schema = StructType([
        StructField("Year", IntegerType(), True),
        StructField("Month", IntegerType(), True),
        StructField("Title", StringType(), True),
        StructField("Authors", ArrayType(StringType()), True),
        StructField("Subjects", ArrayType(StringType()), True),
        StructField("Abstract", StringType(), True)
    ])

    all_data = []

    for year in range(2023, 2024):
        for month in range(1, 2):
            if year == 2023 and month > 11:
                break

            last_day = 28 if month == 2 else 31 if month in [1, 3, 5, 7, 8, 10, 12] else 30

            for day in range(1, last_day + 1):
                papers_data = crawl_papers(year, month, day)
                all_data.extend(papers_data)

    rdd = spark.sparkContext.parallelize(all_data)
    all_papers_df = spark.createDataFrame(rdd, schema=schema)

    # Remove duplicates, keeping the first occurrence
    all_papers_df = all_papers_df.dropDuplicates(subset=["Title"])

    hdfs_path = 'hdfs:///user/maria_dev/archive_store/2023'
    all_papers_df.coalesce(1).write.csv(hdfs_path, header=True, mode='overwrite')

if __name__ == "__main__":
    main()

