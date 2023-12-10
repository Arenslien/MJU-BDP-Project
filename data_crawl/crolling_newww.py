# -*- coding: utf-8 -*-
"""crolling_newww.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1t7dKUSJ2ina5YSZRKH3-0_VsZMt49lqa
"""

df = pd.read_csv('/content/drive/MyDrive/2020.csv')

import pandas as pd

nan_values = df.isnull().sum().sum()

if nan_values > 0:
    print("There are NaN values in the DataFrame.")
    # If you want to see the count of NaN values in each column, you can use:
    # print(df.isnull().sum())
else:
    print("There are no NaN values in the DataFrame.")

data = df.copy()

# 'title' 컬럼의 중복 확인
duplicates = data[data.duplicated('Title')]

if not duplicates.empty:
    print("중복된 데이터가 있습니다:")
    print(duplicates['Title'])
else:
    print("중복된 데이터가 없습니다.")

df[df['Abstract']=='NaN']

print(df[df.duplicated('Title', keep=False)])

df[df['Title']=='Title: Deep Upper Confidence Bound Algorithm for Contextual Bandit Ranking of  Information Selection']

df

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# Function to extract information from a single paper entry
def extract_paper_info(entry):

    title = entry.find("div", class_="list-title mathjax").text.strip()
    authors = [author.text.strip() for author in entry.find_all("a", href=lambda x: x and "/search/cs?searchtype=author" in x)]
    subjects = [subject.text.strip() for subject in entry.find_all("span", class_="primary-subject")]
    abstract = entry.find("p", class_="mathjax").text.strip()

    return title, authors, subjects, abstract

# Function to crawl papers for a given date
def crawl_papers(year, month, day, existing_data):
    url = f"https://arxiv.org/catchup?syear={year}&smonth={month}&sday={day}&num=25&archive=cs&method=with"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    papers = soup.find_all("dd")
    data = []

    for paper in papers:
        title, authors, subjects, abstract = extract_paper_info(paper)
        data.append([year, month, title, authors, subjects, abstract])

    return data

# Main loop to crawl papers for multiple dates
columns = ["Year", "Month", "Title", "Authors", "Subjects", "Abstract"]
all_data = []

for year in range(2013, 2015):
    for month in range(1, 13):
        if year == 2023 and month > 11:
            break

        # Determine the last day of the month
        last_day = 28 if month == 2 else 31 if month in [1, 3, 5, 7, 8, 10, 12] else 30

        for day in range(1, last_day + 1):
            papers_data = crawl_papers(year, month, day, all_data)
            all_data.extend(papers_data)

# Create a DataFrame from the collected data
df = pd.DataFrame(all_data, columns=columns)

# Remove duplicates, keeping the first occurrence
df = df.drop_duplicates(subset="Title", keep="first")

# Save the DataFrame as a CSV file without the "Day" column
csv_path = '/content/drive/MyDrive/2013to14.csv'
df.to_csv(csv_path, index=False, columns=["Year", "Month", "Title", "Authors", "Subjects", "Abstract"])


# Display the DataFrame
print(df.head())