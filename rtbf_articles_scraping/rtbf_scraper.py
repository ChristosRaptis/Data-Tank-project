"""
Scrape "https://www.rtbf.be/site-map/articles.xml to get
the url, ttile, text and date of each article
"""
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import ssl  # Use this import only if you get a Certificate error in Mac
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

ssl._create_default_https_context = ssl._create_unverified_context

<<<<<<< HEAD

<<<<<<< HEAD
=======
start_time = time.perf_counter()


>>>>>>> Christos_branch
def find_article_title(url: str) -> str:
    response = requests.get(url)
    soup = bs(response.content, "html.parser")
=======
start_time = time.perf_counter()


def find_article_title(soup) -> str:
>>>>>>> Christos_branch
    try:
        article_title = soup.find("h1").text
    except:
        article_title = soup.find("h2").text
    return article_title


def find_article_text(soup) -> str:
    paragraphs = [p.text.strip() for p in soup.find_all("p", attrs={"class": None})]
    article_text = "".join(paragraphs)
    return article_text


<<<<<<< HEAD
# Fetch sitemap
=======
print("Creating dataframe from sitemap ...")
>>>>>>> Christos_branch
sitemap = pd.read_xml("https://www.rtbf.be/site-map/articles.xml")

print("Removing unecessary columns and keeping loc and lastmod ...")
df = sitemap.drop(["changefreq", "news", "image"], axis=1)
df.rename(columns={"loc": "url", "lastmod": "date"}, inplace=True)

<<<<<<< HEAD
# Add 'language' column
df["language"] = "fr"

# Add 'article_title' column
df["article_title"] = df["source_url"].apply(find_article_title)

# Add 'article_text' column
df["article_text"] = df["source_url"].apply(find_article_text)

# Rearange column order
df = df.loc[:, ["source_url", "article_title", "article_text", "date", "language"]]

# export to csv
df.to_csv("data/rtbf_articles.csv")
=======
print("Adding language column ...")
df["language"] = "fr"

print("Creating list of articles ...")
articles = df.to_dict(orient="records")

print("Adding title and text to articles ...")
for index, article in enumerate(articles, start=1):
    print(f"  Updating article {index} of {len(articles)}")
    response = requests.get(article["url"])
    soup = bs(response.content, "html.parser")
    print("    Adding title ...")
    article["title"] = find_article_title(soup)
    print("    Adding text ...")
    article["text"] = find_article_text(soup)

print("Connecting to database ...")
client = MongoClient(os.getenv("MONGODB_URI"))
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
>>>>>>> Christos_branch
