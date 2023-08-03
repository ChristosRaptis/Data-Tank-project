import requests
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs


def fetch_cookie(url):
    session = requests.Session()
    cookie = session.get(url).cookies
    return cookie


def fetch_title(url):
    response = requests.get(url)
    soup = bs(response.content, "html.parser")
    h1_tag = soup.find("h1")
    if h1_tag:
        text = h1_tag.get_text(strip=True)
        return text
    else:
        return ""


def fetch_article(url):
    response = requests.get(url)
    soup = bs(response.content, "html.parser")
    elements = soup.find_all("p")
    text_list = [element.get_text() for element in elements]
    return "\n".join(text_list)


def find_published_date(url):
    response = requests.get(url, cookies=fetch_cookie(url))
    soup = bs(response.content, "html.parser")
    published_time = soup.find("meta", property="article:published_time")
    published_date = published_time["content"]
    return published_date


def main():
    sitemaps = pd.read_xml("https://www.hln.be/sitemap-news.xml")
    df = sitemaps.drop(["news", "image", "lastmod"], axis=1)
    df.rename(columns={"loc": "source_url"}, inplace=True)
    df["published_date"] = df["source_url"].apply(find_published_date)
    df["article_title"] = df["source_url"].apply(find_title_article)
    df["article_text"] = df["source_url"].apply(find_article)
    df = df.loc[:, ["source_url", "article_title", "article_text", "published_date"]]
    df.to_csv("../hln_scrapper/Data/hln_articles.csv")


if __name__ == "__main__":
    main()
