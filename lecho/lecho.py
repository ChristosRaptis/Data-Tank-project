# NOT WORKING

import requests
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re

# import ssl

# ssl._create_default_https_context = ssl._create_unverified_context
from tqdm import tqdm
import functools

tqdm.pandas()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
}


def main():
    session = requests.Session()
    sitemap = session.get("https://www.lecho.be/news/sitemap.xml", headers=HEADERS)
    sitemaps = pd.read_xml(sitemap.text)
    df = sitemaps.drop(["news"], axis=1)
    df.rename(columns={"loc": "source_url"}, inplace=True)
    df["published_date"] = df["source_url"].progress_apply(
        functools.partial(find_published_date, session=session)
    )
    df["article_title"] = df["source_url"].progress_apply(
        functools.partial(find_article_title, session=session)
    )
    df["article_text"] = df["source_url"].progress_apply(
        functools.partial(find_article_text, session=session)
    )
    df = df.loc[:, ["source_url", "article_title", "article_text", "published_date"]]
    df.to_csv("lecho_articles.csv")


def find_article_title(url: str, session) -> str:
    response = session.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")
    title = soup.find("h1").text
    return title


def find_article_text(url: str, session) -> str:
    response = session.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")
    article = soup.find_all("p", attrs={"class": None})
    articles = [p.text for p in article]
    cleaned_articles = " ".join(articles)
    return cleaned_articles


def find_published_date(url: str, session) -> str:
    response = session.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")
    meta = soup.find("meta", property="article:published_time")
    published_date = meta["content"]
    date_pattern = r"\d{4}-\d{2}-\d{2}"
    date_match = re.search(date_pattern, published_date)
    date = date_match.group()
    return date


if __name__ == "__main__":
    main()
