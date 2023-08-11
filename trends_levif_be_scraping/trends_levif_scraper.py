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
    # df = df.iloc[0:3]

    # Keep only the 'loc' and 'lastmod' columns and rename them
    # df = df.drop(["image"], axis=1)
    df.rename(columns={"loc": "source_url"}, inplace=True)

    # Keep only the date portion of the 'last_modified_date' column
    # df["last_modified_date"].replace({r"T.+": ""}, inplace=True, regex=True)

    # Add 'article_title' column
    df["article_title"] = df["source_url"].progress_apply(functools.partial(find_article_title, session=session))

    # Add 'article_text' column
    df["article_text"] = df["source_url"].progress_apply(functools.partial(find_article_text, session=session))

    df["publication_date"] = df["source_url"].progress_apply(functools.partial(release_date, session=session))

    # Rearange coluln order
    df = df.loc[:, ["source_url", "article_title",
                    "article_text", "publication_date"]]

    # export to csv
    df.to_csv(f"{file_name}")


scrape_articles()
