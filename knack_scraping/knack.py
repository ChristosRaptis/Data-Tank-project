import requests
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import json
import pymongo
from tqdm import tqdm
import functools
import os
from dotenv import load_dotenv

load_dotenv()

# import ssl
# ssl._create_default_https_context = ssl._create_unverified_context

tqdm.pandas()


def main():
    with requests.Session() as session:
        sitemaps = pd.read_xml("https://www.knack.be/news-sitemap.xml")
        df = sitemaps.drop(["news"], axis=1)
        df.rename(columns={"loc": "url"}, inplace=True)
        df["date"] = df["url"].progress_apply(
            functools.partial(find_published_date, session=session)
        )
        df["title"] = df["url"].progress_apply(
            functools.partial(find_article_title, session=session)
        )
        df["text"] = df["url"].progress_apply(
            functools.partial(find_article_text, session=session)
        )
        df = df.loc[:, ["url", "title", "text", "date"]]

        mongodb_url = os.getenv("MONGODB_URI")
        database_name = "bouman_datatank"
        collection_name = "articles"
        data_to_insert = df.to_dict(orient="records")
        print(data_to_insert)
        client = pymongo.MongoClient(mongodb_url)
        database = client[database_name]
        collection = database[collection_name]
        collection.insert_many(data_to_insert)


def find_article_title(url: str, session=None) -> str:
    response = session.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    title = soup.find("h1").text
    return title


def find_article_text(url: str, session=None) -> str:
    response = session.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    article = soup.find_all("p", attrs={"class": None})
    articles = [p.text for p in article]
    cleaned_articles = " ".join(articles)
    return cleaned_articles


def find_published_date(url: str, session=None) -> str:
    response = session.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    script = soup.find("script", {"type": "application/ld+json"})
    data = json.loads(script.text, strict=False)
    accessing_list = data["@graph"]
    accessing_dict = accessing_list[0]
    published_date = accessing_dict["datePublished"]
    date_pattern = r"\d{4}-\d{2}-\d{2}"
    date_match = re.search(date_pattern, published_date)
    date = date_match.group()
    return date


if __name__ == "__main__":
    main()
