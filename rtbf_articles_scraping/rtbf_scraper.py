"""
Scrape "https://www.rtbf.be/site-map/articles.xml to get
the url, ttile, text and date of each article
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import json
# Use the below import only if you get a Certificate error in Mac
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def find_article_title(url: str) -> str:
    response = requests.get(url)
    soup = bs(response.content, "html.parser")
    article_title = soup.find("h1").text
    return article_title

def find_article_text(url: str) -> str:
    response = requests.get(url)
    soup = bs(response.content, "html.parser")
    paragraphs = [p.text for p in soup.find_all("p", attrs={"class": None})]
    article_text = "".join(paragraphs)
    return article_text

# Fetch sitemap
sitemap = pd.read_xml("https://www.rtbf.be/site-map/articles.xml")

# Keep only the 'loc' and 'lastmod' columns and rename them
df = sitemap.drop(["changefreq", "news", "image"], axis=1)
df.rename(columns={"loc": "source_url", "lastmod": "date"}, inplace=True)

# Add 'language' column
df['language'] = 'fr'

# Add 'article_title' column
df['article_title'] = df['source_url'].apply(find_article_title)

# Add 'article_text' column
df['article_text'] = df['source_url'].apply(find_article_text)

# Rearange column order
df = df.loc[:, ['source_url', 'article_title', 'article_text', 'date', 'language']]

# export to csv
df.to_csv('data/rtbf_articles.csv')