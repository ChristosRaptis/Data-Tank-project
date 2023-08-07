import functools
import requests
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import pymongo
from pymongo import MongoClient


tqdm.pandas()


def fetch_cookie(url, session):  # will fetch cookie from hln so we can see content
    cookie = session.get(url).cookies
    return cookie


def fetch_title(url, session):  # will fetch the title article using session
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    title = soup.find_all("h1")
    article_title = title[0].text.strip()
    return article_title


def fetch_article(url, session):  # will fetch the article using session
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    elements = soup.find_all("p")
    text_list = [element.get_text() for element in elements]
    return "\n".join(text_list)


def find_published_date(
    url, session
):  # will fetch the published date of related article
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    published_time = soup.find("meta", property="article:published_time")
    published_date = published_time.get("content", None)

    return published_date


def main():
    """input xml sitemaps to be read as a dataframe, droping and renaming columns, and will use all previous functions
    to create a database that will be send to a Mango database"
    """
    session = requests.Session()
    fetch_cookie("https://www.hln.be", session)

    sitemaps = pd.read_xml("https://www.hln.be/sitemap-news.xml")
    df = sitemaps.drop(["news", "image", "lastmod"], axis=1)
    df.rename(columns={"loc": "source_url"}, inplace=True)
    df["published_date"] = df["source_url"].progress_apply(
        functools.partial(find_published_date, session=session)
    )

    df["article_title"] = df["source_url"].progress_apply(
        functools.partial(fetch_title, session=session)
    )
    df["article_text"] = df["source_url"].progress_apply(
        functools.partial(fetch_article, session=session)
    )

    df = df.loc[:, ["source_url", "article_title", "article_text", "published_date"]]

    # Create a connection with MangoDB
    mongodb_url = "mongodb://bouman:80um4N!@ec2-15-188-255-64.eu-west-3.compute.amazonaws.com:27017/"
    database_name = "bouman_datatank"
    collection_name = "articles"
    print(df)
    data_to_insert = df.to_dict(orient="records")
    print(data_to_insert)
    client = pymongo.MongoClient(mongodb_url)
    database = client[database_name]
    collection = database[collection_name]
    collection.insert_many(data_to_insert)


if __name__ == "__main__":
    main()
