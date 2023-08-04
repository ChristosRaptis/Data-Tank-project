import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import ssl
import re
ssl._create_default_https_context = ssl._create_unverified_context
from tqdm import tqdm
import functools

tqdm.pandas()

def main():
    with requests.Session() as session: 
        sitemap = pd.read_xml('https://www.sudinfo.be/sites/default/files/sitemaps/sitemapnews-0.xml')
        df = sitemap.drop(["news", "image"], axis=1)
        df.rename(columns={"loc": "source_url"}, inplace=True)
        df['published_date'] = df['source_url'].progress_apply(functools.partial(find_published_date, session=session))
        df['article_title'] = df['source_url'].progress_apply(functools.partial(find_article_title, session=session))
        df['article_text'] = df['source_url'].progress_apply(functools.partial(find_article_text, session=session))
        df = df.loc[:, ['source_url', 'article_title', 'article_text', 'published_date']]
        df.to_csv('sudinfo_articles.csv')

def find_article_title(url: str, session = None) -> str:
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    title = soup.find("h1").text
    text = title.strip()
    return text

def find_published_date(url:str, session = None) -> str:
    response = session.get(url)   
    soup = bs(response.content, "html.parser")
    date = soup.find("time").text
    date_pattern = r"\d{2}/\d{2}/\d{4}" 
    date_match = re.search(date_pattern, date)
    date = date_match.group()
    day, month, year = date.split("/")
    date= f"{year}-{month}-{day}"
    return date

def find_article_text(url: str, session = None) -> str:
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    paragraph = soup.find_all("r-article--chapo", attrs={"class": None})
    paragraphs = [p.text for p in paragraph]
    cleaned_paragraphs = ' '.join(paragraphs)
    return cleaned_paragraphs

if __name__ == "__main__":
    main()