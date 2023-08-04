import functools
import requests
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm

tqdm.pandas()


def fetch_cookie(url, session):
    cookie = session.get(url).cookies
    return cookie


def fetch_title(url, session):
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    title = soup.find_all("h1")
    article_title = title[0].text.strip()
    return article_title


def fetch_article(url, session):
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    elements = soup.find_all("p")
    text_list = [element.get_text() for element in elements]
    return "\n".join(text_list)


def find_published_date(url, session):
    response = session.get(url)
    soup = bs(response.content, "html.parser")
    published_time = soup.find("meta", property="article:published_time")
    published_date = published_time.get("content", None)
    return published_date


def main():
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
    df.to_csv("hln_articles.csv")


if __name__ == "__main__":
    main()
