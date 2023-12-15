import nest_asyncio
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import csv

nest_asyncio.apply()

# 논문 정보를 저장할 리스트
all_papers = []

async def fetch_page(session, url):
    async with session.get(url) as response:
        return await response.text()

async def scrape_arxiv(session, year, month, day):
    url = f"https://arxiv.org/catchup?syear={year}&smonth={month}&sday={day}&num=1500&archive=cs&method=with"

    async with session.get(url) as response:
        page_html = await response.text()

    soup = BeautifulSoup(page_html, 'html.parser')
    paper_items = soup.find_all('dd')  # 각 논문을 나타내는 부분을 선택

    for item in paper_items:
        # 제목
        title_tag = item.find('div', class_='list-title mathjax')
        title = title_tag.get_text(separator=' ', strip=True) if title_tag else "Title not found"

        # 저자
        authors_tag = item.find('div', class_='list-authors')
        authors = authors_tag.get_text(separator=' ', strip=True) if authors_tag else "Authors not found"

        # 주제
        subjects_tag = item.find('div', class_='list-subjects')
        subjects = subjects_tag.get_text(separator=' ', strip=True) if subjects_tag else "Subjects not found"

        # 초록
        abstract_tag = item.find('p', class_='mathjax')
        abstract = abstract_tag.get_text(separator=' ', strip=True) if abstract_tag else "Abstract not found"

        # 각 논문 정보를 리스트에 저장
        paper_info = [title, authors, subjects, abstract]
        all_papers.append(paper_info)


async def main():
    async with aiohttp.ClientSession() as session:
        years = list(range(2023, 2022, -1))  # 2023년만
        # years = list(range(2022, 2021, -1))  # 2022년만
        # years = list(range(2021, 2020, -1))  # 2021년만
        months = list(range(1, 13))  # 1월부터 12월까지

        for year in years:
            for month in months:
                # 2023년은 11월까지만 크롤링
                if year == 2023 and month > 11:
                    break
                # 각 월의 일 수를 고려하여 크롤링
                for day in range(1, 32):
                    await scrape_arxiv(session, year, month, day)
                    print(all_papers[-1])  # 마지막으로 추가된 논문 정보 출력

if __name__=="__main__":
    # 비동기 코드 실행
    await main()

    # 결과를 CSV 파일로 저장
    with open('arxiv_CS_2023.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)

        # 헤더 추가
        csv_writer.writerow(["Title", "Authors", "Subjects", "Abstract"])

        # 논문 정보 쓰기
        csv_writer.writerows(all_papers)
        
# 2022년도 완료
# 2021년도 완료