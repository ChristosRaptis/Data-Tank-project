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
import json

load_dotenv()


def fetch_cookie(url, session):  # will fetch cookie from hln so we can see content
    cookie = session.get(url).cookies
    return cookie

def get_links(url, session):
    url = "https://www.hln.be/sitemap-news.xml"
    pages = requests.get(url, cookies= fetch_cookie(url, session)).content

    # fetch_cookie(url, session)
    soup = bs(pages, "html.parser")
    links = []
    all_links = soup.find_all("loc")

    for link in all_links:
        links.append(link.text)
    return links


def fetch_title_article_date(links, session):  # will fetch the title article using session
    articles = []

    for i, link in enumerate(links) :
        empty_dico = {}
        response = session.get(link)
        soup = bs(response.content, "html.parser")
        
        title = soup.find_all("h1")
        if title :
            article_title = title[0].text.strip()
            empty_dico["title"] = article_title
        else :
            print("no title")

        elements = soup.find_all("p")
        if elements :
            text_list = [element.get_text() for element in elements]
            empty_dico["article"] = "\n".join(text_list)
        else :
            print(("no articles"))

        published_time = soup.find("meta", property="article:published_time")
        if published_time:
            published_date = published_time.get("content", None)
            empty_dico["date"] = published_date
        else :
            print("no date")

        articles.append(empty_dico)
        print(f"Appending article {i+1} / {len(links)}")
    return articles

def save_json(articles):
    with open(articles, "w") as f:
        json.dumps(f)

def mangodb_connection(articles):
    # Create a connection with MangoDB and upload database
    mongodb_url = os.getenv("MONGODB_URI")
    database_name = "bouman_datatank"
    collection_name = "articles"
    data_to_insert = df.to_dict(orient="records")
    client = pymongo.MongoClient(mongodb_url)
    database = client[database_name]
    collection = database[collection_name]
    collection.insert_many(data_to_insert)

def main():
    """input xml sitemaps to be read as a dataframe, droping and renaming columns,
    and will use all previous functions to create a database that will be send
    to a Mango database to be automatize by Airflow."
    """

    url = "https://www.hln.be/sitemap-news.xml"
    session = requests.Session()
    fetch_cookie("https://www.hln.be", session)
    links = get_links(url, session)
    articles = fetch_title_article_date(links, session)
    save_json(articles)
    # mangodb_connection(articles)

if __name__ == "__main__":
    main()
