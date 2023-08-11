import requests
from bs4 import BeautifulSoup
import csv

sitemap_url_lavenir = (
    "https://www.lavenir.net/arc/outboundfeeds/sitemap-news/?outputType=xml"
)


def extract_text_from_website(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    elements = soup.find_all(
        "p", class_="text-left ap-StoryElement ap-StoryElement--mb ap-StoryText"
    )
    text_list = [element.get_text() for element in elements]
    return "\n".join(text_list)


def extract_news_title_from_url(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    h1_tag = soup.find("h1", class_="ap-Title ap-StoryTitle")
    if h1_tag:
        text = h1_tag.get_text(strip=True)
        return text
    else:
        return ""


def extract_sitemap_data(sitemap_url_lavenir):
    response = requests.get(sitemap_url_lavenir)
    sitemap_soup = BeautifulSoup(response.text, "lxml")

    urls = sitemap_soup.find_all("url")

    sitemap_data_lavenir = []
    for url in urls:
        loc = url.find("loc").text
        lastmod = url.find("lastmod").text
        news_title = extract_news_title_from_url(loc)
        news_pub_date = url.find("news:publication_date").text
        news_name = url.find("news:name").text
        news_language = url.find("news:language").text

        scraped_text = extract_text_from_website(loc)
        data = {
            "URL": loc,
            "Last Modification": lastmod,
            "News Title": news_title,
            "News Publication Date": news_pub_date,
            "News Name": news_name,
            "News Language": news_language,
            "News Text": scraped_text,
        }

        sitemap_data_lavenir.append(data)

    return sitemap_data_lavenir


if __name__ == "__main__":
    sitemap_data_lavenir = extract_sitemap_data(sitemap_url_lavenir)

    headers = [
        "URL",
        "Last Modification",
        "News Title",
        "News Publication Date",
        "News Name",
        "News Language",
        "News Text",
    ]
    with open("sitemap_data_lavenir.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(sitemap_data_lavenir)
