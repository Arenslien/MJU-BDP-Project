# 데이터 로드
file_path = "/content/2013to2023.csv"
data = pd.read_csv(file_path)


df = data.copy()


"""# yake model"""

import yake

def get_keywords_yake(text):
    y = yake.KeywordExtractor(lan='en',          # language
                             n = 3,              # n-gram size
                             dedupLim = 0.73,     # deduplicationthresold
                             dedupFunc = 'seqm', #  deduplication algorithm
                             windowsSize = 1,
                             top = 40,           # number of keys
                             features=None)

    keywords = y.extract_keywords(text)
    return keywords

"""# yake_2023"""

t_2023 = df[df['Year']==2023]
t_2023_join = ', '.join(t_2023['Title'])

yake_2023 = get_keywords_yake(t_2023_join)

yake_2023

yake_2023[20:40]

"""# yake_2022"""

t_2022 = df[df['Year']==2022]
t_2022_join = ', '.join(t_2022['Title'])

yake_2022 = get_keywords_yake(t_2022_join)

"""#yake_2021"""

t_2021 = df[df['Year']==2021]
t_2021_join = ', '.join(t_2021['Title'])

yake_2021 = get_keywords_yake(t_2021_join)

pd.DataFrame(yake_2021)

"""#yake_2020"""

t_2020 = df[df['Year']==2020]
t_2020_join = ', '.join(t_2020['Title'])

yake_2020 = get_keywords_yake(t_2020_join)

"""#yake_2019"""

t_2019 = df[df['Year']==2019]
t_2019_join = ', '.join(t_2019['Title'])

yake_2019 = get_keywords_yake(t_2019_join)

pd.DataFrame(yake_2019)

"""#yake_2018"""

t_2018 = df[df['Year']==2018]
t_2018_join = ', '.join(t_2018['Title'])

yake_2018 = get_keywords_yake(t_2018_join)

"""#yake_2017"""

t_2017 = df[df['Year']==2017]
t_2017_join = ', '.join(t_2017['Title'])

yake_2017 = get_keywords_yake(t_2017_join)

"""#yake_2016"""

t_2016 = df[df['Year']==2016]
t_2016_join = ', '.join(t_2016['Title'])

yake_2016 = get_keywords_yake(t_2016_join)

"""#yake_2015"""

t_2015 = df[df['Year']==2015]
t_2015_join = ', '.join(t_2015['Title'])

yake_2015 = get_keywords_yake(t_2015_join)

"""#yake_2014"""

t_2014 = df[df['Year']==2014]
t_2014_join = ', '.join(t_2014['Title'])

yake_2014 = get_keywords_yake(t_2014_join)

"""#yake_2013"""

t_2013 = df[df['Year']==2013]
t_2013_join = ', '.join(t_2013['Title'])

yake_2013 = get_keywords_yake(t_2013_join)

