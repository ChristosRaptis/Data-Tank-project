"""
Scrape "https://trends.levif.be/post-sitemap134.xml to get
the url, ttile, text and date of each article
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import json

# Use the below import only if you get a Certificate error in Mac
# import ssl
# ssl._create_default_https_context = ssl._create_unverified_context


def find_article_title(url: str) -> str:
    response = requests.get(url, cookies=fetch_cookie(url))
    soup = bs(response.content, "html.parser")
    article_title = soup.find("h1").text
    return article_title


def fetch_cookie(url):
    session = requests.Session()
    cookie = session.get(url).cookies
    return cookie


def find_article_text(url: str) -> str:
    response = requests.get(url, cookies=fetch_cookie(url))
    soup = bs(response.content, "html.parser")
    paragraphs = [p for p in soup.find_all("div", attrs={"class": "paywalled"})]
    for paragraph in paragraphs:
        article_text = "".join(paragraph.text).lstrip()
        return article_text


# Fetch sitemap
sitemap = pd.read_xml("https://www.hln.be/sitemap-news.xml")
df = sitemap.head(3)

# Keep only the 'loc' and 'lastmod' columns and rename them
df = df.drop(["image"], axis=1)
df.rename(columns={"loc": "source_url", "lastmod": "last_modified_date"}, inplace=True)

# Keep only the date portion of the 'last_modified_date' column
# df["last_modified_date"].replace({r"T.+": ""}, inplace=True, regex=True)

# Add 'article_title' column
df["article_title"] = df["source_url"].apply(find_article_title)

# Add 'article_text' column
df["article_text"] = df["source_url"].apply(find_article_text)

# Rearange coluln order
df = df.loc[:, ["source_url", "article_title", "article_text", "last_modified_date"]]

# export to csv
df.to_csv("trends_levif_be_scraping/data/trends_levif.csv")
