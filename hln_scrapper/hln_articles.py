import functools
import requests
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import pymongo
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
tqdm.pandas()


def fetch_cookie(url, session):  # will fetch cookie from hln so we can see content
    cookie = session.get(url).cookies
    return cookie


def fetch_title(links, session):  # will fetch the title article using session
    response = session.get(links)
    soup = bs(response.content, "html.parser")
    title = soup.find_all("h1")
    article_title = title[0].text.strip()
    return article_title


def fetch_article(links, session):  # will fetch the article using session
    response = session.get(links)
    soup = bs(response.content, "html.parser")
    elements = soup.find_all("p")
    text_list = [element.get_text() for element in elements]
    return "\n".join(text_list)


def find_published_date(links, session):  # will fetch the published date
    response = session.get(links)
    soup = bs(response.content, "html.parser")
    published_time = soup.find("meta", property="article:published_time")
    published_date = published_time.get("content", None)
    return published_date


def get_links(url, session):
    url = "https://www.hln.be"
    pages = requests.get(url).content
    fetch_cookie(url, session)
    soup = bs(pages, "html.parser")
    links = []
    all_links = soup.find_all("loc")
    for link in all_links:
        links.append(link.text)
    return links


def main():
    """input xml sitemaps to be read as a dataframe, droping and renaming columns,
    and will use all previous functions to create a database that will be send
    to a Mango database to be automatize by Airflow."
    """

    session = requests.Session()
    # fetch_cookie("https://www.hln.be", session)

    sitemaps = pd.read_xml("https://www.hln.be/sitemap-news.xml")

    df = sitemaps.drop(["news", "image", "lastmod"], axis=1)
    df.rename(columns={"loc": "url"}, inplace=True)
    df["date"] = df["url"].progress_apply(
        functools.partial(find_published_date, session=session)
    )
    df["title"] = df["url"].progress_apply(
        functools.partial(fetch_title, session=session)
    )
    df["text"] = df["url"].progress_apply(
        functools.partial(fetch_article, session=session)
    )
    df = df.loc[:, ["url", "title", "text", "date"]]

    # Create a connection with MangoDB and upload database
    mongodb_url = os.getenv("MONGODB_URI")
    database_name = "bouman_datatank"
    collection_name = "articles"
    data_to_insert = df.to_dict(orient="records")
    client = pymongo.MongoClient(mongodb_url)
    database = client[database_name]
    collection = database[collection_name]
    collection.insert_many(data_to_insert)


if __name__ == "__main__":
    main()
