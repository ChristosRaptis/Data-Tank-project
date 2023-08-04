"""
Scrape https://www.lesoir.be/18/sections/le-direct to get
the url, title, text, date and language of each article in the news feed.
"""
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import json
import unicodedata

# Use the below import if you get a Certificate error in Mac
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

start_time = time.perf_counter()

def get_soup(url:str):
    response = requests.get(url)
    soup = bs(response.content, "html.parser")
    return soup

def find_article_title(url: str) -> str:
    soup = get_soup(url)
    article_title = soup.find("h1").text
    return article_title


# selector for geeko.lesoir.be urls
def geeko_selector(url: str) -> tuple:
    soup = get_soup(url)
    div = soup.find("div", attrs={"class": "post-content-area"})
    paragraphs = div.find_all("p")
    if "Suivez Geeko sur Facebook" in paragraphs[-1].text:
        paragraphs = paragraphs[:-1]
    text = ""
    return paragraphs, text


# selector for sosoir.lesoir.be urls
def sosoir_selector(url: str) -> tuple:
    soup = get_soup(url)
    h2 = soup.find("h2", attrs={"class": "chapeau"}).text.strip()
    div = soup.find("div", attrs={"id": "artBody"})
    paragraphs = div.find_all("p")
    if "sosoir.lesoir.be" in paragraphs[-1].text:
        paragraphs = paragraphs[:-1]
    text = h2
    return paragraphs, text


# selector for www.lesoir.be and soirmag.lesoir.be urls
def lesoirmag_selector(url: str) -> tuple:
    soup = get_soup(url)
    article = soup.find("article", attrs={"class": "r-article"})
    paragraphs = article.find_all("p")
    if "www.soirmag.be" in paragraphs[-1].text:
        paragraphs = paragraphs[:-1]
    text = ""
    return paragraphs, text


def find_article_text(url: str) -> str:
    if "sosoir" in url:
        paragraphs, article_text = sosoir_selector(url)
    elif "geeko" in url:
        paragraphs, article_text = geeko_selector(url)
    else:
        paragraphs, article_text = lesoirmag_selector(url)
    for p in paragraphs:
        p_text = unicodedata.normalize("NFKD", p.text.strip())
        article_text += p_text
    return article_text


def find_published_date(url: str) -> str:
    soup = get_soup(url)
    script = soup.find("script", {"type": "application/ld+json"})
    data = json.loads(script.text, strict=False)
    try:
        published_date = data["@graph"][0]["datePublished"]
    except:
        published_date = data["datePublished"]
    return published_date


print("Creating list of urls of news page ...")
soup = get_soup("https://www.lesoir.be/18/sections/le-direct")
urls = []
base_url = "https://www.lesoir.be"
links = soup.select("h3 > a")
for link in links:
    url = link.get("href")
    dict = {"source_url": url if "//" in url else base_url + url}
    urls.append(dict)

print("Creating dataframe ...")
df = pd.DataFrame(urls)

print("Adding article_title column to dataframe ...")
df["article_title"] = df["source_url"].apply(find_article_title)

print("Adding article_text column to dataframe ...")
df["article_text"] = df["source_url"].apply(find_article_text)

print("Adding date column to dataframe ...")
df["date"] = df["source_url"].apply(find_published_date)

print("Adding language column to dataframe ...")
df["language"] = "fr"

print("Extracting data to lesoir_articles.csv ...")
df.to_csv("data/lesoir_articles.csv")

end_time = time.perf_counter()
print(round(end_time - start_time, 2), "seconds")
