"""
Scrape "https://www.rtbf.be/site-map/articles.xml to get
the url, ttile, text and date of each article
"""
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs

# Use the below import only if you get a Certificate error in Mac
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


<<<<<<< HEAD
=======
start_time = time.perf_counter()


>>>>>>> Christos_branch
def find_article_title(url: str) -> str:
    response = requests.get(url)
    soup = bs(response.content, "html.parser")
    try:
        article_title = soup.find("h1").text
    except:
        article_title = soup.find("h2").text   
    return article_title


def find_article_text(url: str) -> str:
    response = requests.get(url)
    soup = bs(response.content, "html.parser")
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
df.rename(columns={"loc": "source_url", "lastmod": "date"}, inplace=True)

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

print("Adding article_title column ...")
df["article_title"] = df["source_url"].apply(find_article_title)

print("Adding article_text column ...")
df["article_text"] = df["source_url"].apply(find_article_text)

print(" Rearranging column order ...")
df = df.loc[:, ["source_url", "article_title", "article_text", "date", "language"]]

print("Extracting to rtbf_articles.csv ...")
df.to_csv("data/rtbf_articles.csv")

end_time = time.perf_counter()
print(round(end_time - start_time, 2), "seconds")
>>>>>>> Christos_branch
