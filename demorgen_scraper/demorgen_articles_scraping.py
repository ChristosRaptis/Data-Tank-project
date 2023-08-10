from bs4 import BeautifulSoup
import requests
import pandas as pd
import json
from unidecode import unidecode
from tqdm.contrib.concurrent import thread_map
from functools import partial
import itertools
from concurrent.futures import ThreadPoolExecutor
from dateutil import parser
import os
import pymongo

def get_title(soup): # Extract title 
    return soup.find_all('title')[0].text.split('|')[0].strip()

def get_text(soup): # Extract the article's paragraphs
    paragraphs = ''
    p_intro = soup.find_all('p', {"class": "artstyle__intro"}) # article intro
    if p_intro:
        paragraphs  += p_intro[0].text
    p_tags = soup.find_all('p', {"class": "artstyle__paragraph"})
    for parag in p_tags:
        paragraphs += parag.text # adding article's paragraphs
    return paragraphs

def get_date(soup): # Extract the published date
    script = soup.find('script', {"type": "application/ld+json"})
    data = json.loads(script.text, strict=False)
    return data['@graph'][1]['datePublished']


def get_links(feeds_url): # Extract links from news feed
    page_divs = requests.get(feeds_url).content
    soup = BeautifulSoup(page_divs, "html.parser")
    links = []
    all_links = soup.find_all("loc")
    for link in all_links:
        links.append(link.text)
    return links

def get_article(url, session):

    try:
        page_html = session.get(url).content
        soup = BeautifulSoup(page_html, "html.parser")
        page_type = soup.find_all('span', {"class": "artstyle__labels__label"})
        page_blog = soup.find_all('span', {"class": "artstyle__labels__live"})
        if page_type:
            if (page_type[0].text == 'Cartoon'):
                return 
            else:
                article_title = get_title(soup)
                article_text = get_text(soup)
                published_date = parser.parse(get_date(soup))
                row = [[url,article_text,published_date,article_title,'NL']]
                df = pd.DataFrame(row,columns = ["url","text","date","title","language"])
        else:
            if page_blog:
                return
            else:
                article_title = get_title(soup)
                article_text = get_text(soup)
                published_date = parser.parse(get_date(soup))
                row = [[url,article_text,published_date,article_title,'NL']]
                df = pd.DataFrame(row,columns = ["url", "text","date","title", "language"])    
        return df
    
    except Exception as e:
        print(type(e))
        print(e)
        return e

if __name__ == "__main__":

    feeds_url = 'https://www.demorgen.be/sitemaps/news.xml'
    links  = get_links(feeds_url)

    with requests.Session() as session:
        news_feed = pd.concat(df for df in thread_map(partial(get_article, session=session), links) 
                               if isinstance(df, pd.DataFrame))
    
news_feed_dict= news_feed.to_dict('records')
    
mongodb_url = os.getenv("MONGODB_URL")
database_name = "bouman_datatank"
collection_name = "articles"
client = pymongo.MongoClient(mongodb_url)
database = client[database_name]
collection = database[collection_name]

for article in news_feed_dict:
        article_url = article["url"]
        existing_article = collection.find_one({"url": article_url})
        if not existing_article:
            collection.insert_one(article)

client.close()
#----------------------------
# mongodb_url = 'mongodb://localhost:27017'
# database_name = "bouman_datatank"
# collection_name = "articles"
# client = pymongo.MongoClient(mongodb_url)
# database = client[database_name]
# collection = database[collection_name]

# for article in news_feed_dict:
#         article_url = article["url"]
#         existing_article = collection.find_one({"url": article_url})
#         if not existing_article:
#             collection.insert_one(article)

# client.close()
#------------------------------