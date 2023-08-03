import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import ssl
import json
import re
ssl._create_default_https_context = ssl._create_unverified_context

def main():
    sitemap = pd.read_xml('https://www.lalibre.be/arc/outboundfeeds/sitemap-news/?outputType=xml')
    df = sitemap.drop(["news", "image"], axis=1)
    df.rename(columns={"loc": "source_url", "lastmod": "last_modified_date"}, inplace=True)
    df["last_modified_date"].replace({r"T.+": ""}, inplace=True, regex=True)
    df['published_date'] = df['source_url'].apply(find_published_date)
    df['article_title'] = df['source_url'].apply(find_article_title)
    df['article_text'] = df['source_url'].apply(find_article_text)
    df = df.loc[:, ['source_url', 'article_title', 'article_text', 'published_date', 'last_modified_date']]
    df.to_csv('lalibre_articles.csv')

def find_article_title(url: str) -> str:
    response = requests.get(url)
    soup = bs(response.content, "html.parser")
    title = soup.find_all("h1")
    article_title = title[0].text.strip() if title else "Unknown"
    return article_title

def find_article_text(url: str) -> str:
    response = requests.get(url)
    soup = bs(response.content, "html.parser")
    paragraphs = [p.text for p in soup.select('p[class~="ap-StoryText"]')]
    article_text = "".join(paragraphs)
    return article_text

def find_published_date(url:str) -> str:
    response = requests.get(url)
    soup = bs(response.content, "html.parser")
    date = [p.text for p in soup.find('p')]
    data = " ".join(date)
    date_pattern = r"\d{2}-\d{2}-\d{4}" 
    date_match = re.search(date_pattern, data)
    date = date_match.group()
    day, month, year = date.split("-")
    date = f"{year}-{month}-{day}"  
    return date

if __name__ == "__main__":
    main()
