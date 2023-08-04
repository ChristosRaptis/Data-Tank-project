import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import re
import json
from tqdm import tqdm
import functools

tqdm.pandas()

def main():
    with requests.Session() as session: 
        sitemap = pd.read_xml('https://www.levif.be/news-sitemap.xml')
        df = sitemap.drop(["news"], axis=1)
        df.rename(columns={"loc": "source_url"}, inplace=True)
        df['published_date'] = df['source_url'].progress_apply(functools.partial(find_published_date, session=session))
        df['article_title'] = df['source_url'].progress_apply(functools.partial(find_article_title, session=session))
        df['article_text'] = df['source_url'].progress_apply(functools.partial(find_article_text, session=session))
        df = df.loc[:, ['source_url', 'article_title', 'article_text', 'published_date', 'last_modified_date']]
        df.to_csv('lalibre_articles.csv')

def find_article_title(url: str, session = None) -> str:
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    title = soup.find_all("h1")
    article_title = title[0].text.strip()
    return article_title

def find_article_text(url: str, session = None) -> str:
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    article = soup.find("p").text
    return article

def find_published_date(url:str, session = None) -> str:
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    script = soup.find("script", {"type": "application/ld+json"})
    data = json.loads(script.text, strict=False)
    accessing_list = data["@graph"]
    accessing_dict = accessing_list[0]
    published_date = accessing_dict["datePublished"]
    date_pattern = r"\d{2}-\d{2}-\d{4}" 
    date_match = re.search(date_pattern, data)
    date = date_match.group()
    day, month, year = date.split("-")
    date = f"{year}-{month}-{day}"  
    return date

if __name__ == "__main__":
    main()
