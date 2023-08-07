"""
Scrape https://www.lesoir.be/18/sections/le-direct to get
the url, title, text, date and language of each article in the news feed.
"""
import time
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import json
import unicodedata
from pymongo import MongoClient
from tqdm import tqdm
import ssl  # Use this import if you get a Certificate error in Mac

tqdm.pandas()

ssl._create_default_https_context = ssl._create_unverified_context

start_time = time.perf_counter()


def get_soup(url: str):
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
        paragraphs, text = sosoir_selector(url)
    elif "geeko" in url:
        paragraphs, text = geeko_selector(url)
    else:
        paragraphs, text = lesoirmag_selector(url)
    for p in paragraphs:
        p_text = unicodedata.normalize("NFKD", p.text.strip())
        text += p_text
    return text


def find_published_date(url: str) -> str:
    soup = get_soup(url)
    script = soup.find("script", {"type": "application/ld+json"})
    data = json.loads(script.text, strict=False)
    try:
        published_date = data["@graph"][0]["datePublished"]
    except:
        published_date = data["datePublished"]
    date = datetime.strptime(published_date, "%Y-%m-%d %H:%M:%S%z")     
    return date


print("Creating list of urls of news page ...")
soup = get_soup("https://www.lesoir.be/18/sections/le-direct")
articles = []
base_url = "https://www.lesoir.be"
links = soup.select("h3 > a")
for link in links:
    url = link.get("href")
    dict = {"url": url if "//" in url else base_url + url}
    dict['text'] = find_article_text(dict['url'])
    dict['date'] = find_published_date(dict['url'])
    dict['title'] = find_article_title(dict['url'])
    dict['language'] = 'fr'
    articles.append(dict)

# print("Creating dataframe ...")
# df = pd.DataFrame(urls)

# print("Adding text column to dataframe ...")
# df["text"] = df["url"].progress_apply(find_article_text)

# print("Adding date column to dataframe ...")
# df["date"] = df["url"].progress_apply(find_published_date)
# df["date"] = pd.to_datetime(df["date"])

# print("Adding title column to dataframe ...")
# df["title"] = df["url"].progress_apply(find_article_title)

# print("Adding language column to dataframe ...")
# df["language"] = "fr"

# print("Creating dictionary from dataframe ...")
# articles = df.to_dict(orient="records")

print("Connecting to database ...")
client = MongoClient()
client = MongoClient(
    "mongodb://bouman:80um4N!@ec2-15-188-255-64.eu-west-3.compute.amazonaws.com:27017/"
)
db = client["bouman_datatank"]
collection = db["articles"]

print("Checking if articles already exist in database, if not adding them ...")
for article in articles:
    if collection.find_one({"url": {"$eq": article["url"]}}):
        print(f"url: {article['url']}")
        print("An article with this url already exists.")
    else:
        print("Adding new article ...")
        collection.insert_one(article)

print("Closing connection to database ...")
client.close()

end_time = time.perf_counter()
print(round(end_time - start_time, 2), "seconds")
