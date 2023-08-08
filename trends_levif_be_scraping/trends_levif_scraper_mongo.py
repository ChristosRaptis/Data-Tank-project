"""
Scrape "https://trends.levif.be/news-sitemap.xml to get
the url, ttile, text and date of each article
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import json
from tqdm import tqdm 
import functools
import pymongo

tqdm.pandas()


# Use the below import only if you get a Certificate error in Mac
# import ssl
# ssl._create_default_https_context = ssl._create_unverified_context


def release_date(url: str, session) -> str:
    rs = session.get(url).text
    soup = bs(rs, "html.parser")
    script = soup.find("script", {"type": "application/ld+json"})
    data = json.loads(script.text, strict=False)
    accessing_list = data["@graph"]
    accessing_dict = accessing_list[0]
    published_date = accessing_dict["datePublished"]
    return published_date


def find_article_title(url: str, session) -> str:
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    article_title = soup.find("h1").text
    return article_title


def find_article_text(url: str, session) -> str:
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    paragraphs = [p for p in soup.find_all(
        "div", attrs={"class": "paywalled"})]
    article_text = ""
    for paragraph in paragraphs:
        article_text += "".join(paragraph.text).strip().replace("\n", " ")
    return article_text


def scrape_articles(file_name: str = "trends_levif.csv"):
    # Fetch sitemap
    session = requests.Session()
    df = pd.read_xml("https://trends.levif.be/news-sitemap.xml")
    df = df.iloc[0:1]

    # Keep only the 'loc' and 'lastmod' columns and rename them
    # df = df.drop(["image"], axis=1)
    df.rename(columns={"loc": "url"}, inplace=True)

    # Keep only the date portion of the 'last_modified_date' column
    # df["last_modified_date"].replace({r"T.+": ""}, inplace=True, regex=True)

    # Add 'article_title' column
    df["title"] = df["url"].progress_apply(functools.partial(find_article_title, session=session))

    # Add 'article_text' column
    df["text"] = df["url"].progress_apply(functools.partial(find_article_text, session=session))

    df["date"] = df["url"].progress_apply(functools.partial(release_date, session=session))

    # Rearange coluln order
    df = df.loc[:, ["url", "date", "text", "title"]]

    # export to csv
    #df.to_csv(f"{file_name}")
    #mongodb_url = "mongodb://host.docker.internal:27017/"
    mongodb_url = "mongodb://bouman:80um4N!@ec2-15-188-255-64.eu-west-3.compute.amazonaws.com:27017/"
    
    database_name = "bouman_datatank"
    collection_name = "articles"
    print(df)
    data_to_insert = df.to_dict(orient='records')
    print(data_to_insert)
    client = pymongo.MongoClient(mongodb_url)
    database = client[database_name]
    collection = database[collection_name]

    collection.insert_many(data_to_insert)

    client.close()


scrape_articles()
