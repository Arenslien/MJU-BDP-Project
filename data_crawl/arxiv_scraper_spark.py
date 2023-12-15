
from pyspark.sql import SparkSession
from bs4 import BeautifulSoup
import requests

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

def scrape_arxiv(year, month, day, page_html):
    soup = BeautifulSoup(page_html, 'html.parser')
    paper_items = soup.find_all('dd')
    all_papers = []

    for item in paper_items:
        title_tag = item.find('div', class_='list-title mathjax')
        title = title_tag.get_text(separator=' ', strip=True) if title_tag else "Title not found"
        authors_tag = item.find('div', class_='list-authors')
        authors = authors_tag.get_text(separator=' ', strip=True) if authors_tag else "Authors not found"
        subjects_tag = item.find('div', class_='list-subjects')
        subjects = subjects_tag.get_text(separator=' ', strip=True) if subjects_tag else "Subjects not found"
        abstract_tag = item.find('p', class_='mathjax')
        abstract = abstract_tag.get_text(separator=' ', strip=True) if abstract_tag else "Abstract not found"
        paper_info = [year, month, title, authors, subjects, abstract]
        all_papers.append(paper_info)

    return all_papers

def main():
    spark = SparkSession.builder.appName("ArxivScraper").getOrCreate()
    years = list(range(2021, 2020, -1))
    months = list(range(1, 2))
    all_papers_rdd = spark.sparkContext.parallelize([])

    for year in years:
        for month in months:
            if year == 2023 and month > 11:
                break

            for day in range(1, 2):
                url = f"https://arxiv.org/catchup?syear={year}&smonth={month}&sday={day}&num=1500&archive=cs&method=with"
                page_html = fetch_page(url)
                
                if page_html is not None:
                    papers = scrape_arxiv(year, month, day, page_html)
                    all_papers_rdd = all_papers_rdd.union(spark.sparkContext.parallelize(papers))

    all_papers_df = spark.createDataFrame(all_papers_rdd, ["Year", "Month", "Title", "Authors", "Subjects", "Abstract"])

    all_papers_df = all_papers_df.dropDuplicates(["Title"])
    hdfs_path = 'hdfs:///user/maria_dev/archive_store/2021/all_papers'
    all_papers_df.coalesce(1).write.csv(hdfs_path, header=True, mode='overwrite')

if __name__ == "__main__":
    main()

