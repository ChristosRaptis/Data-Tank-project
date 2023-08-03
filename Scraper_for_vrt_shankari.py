import requests
from bs4 import BeautifulSoup
import csv

sitemap_url_vrt = "https://www.vrt.be/vrtnws/nl.news-sitemap.xml"

def extract_text_from_url(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    all_text_divs = soup.find_all('div', class_='text')
    text_list = []

    for text_div in all_text_divs:
        p_tag = text_div.find('p')
        if p_tag:
            text = p_tag.get_text(strip=True)
            text_list.append(text)

    return "\n".join(text_list)

def extract_sitemap_data(sitemap_url):
    response = requests.get(sitemap_url)
    sitemap_soup = BeautifulSoup(response.text, "lxml")

    urls = sitemap_soup.find_all("url")

    sitemap_data = []
    for url in urls:
        loc = url.find("loc").text
        lastmod_tag = url.find("lastmod")
        lastmod = lastmod_tag.text if lastmod_tag else None

        news_title_tag = url.find("news:title")
        news_title = news_title_tag.text if news_title_tag else None

        news_pub_date_tag = url.find("news:publication_date")
        news_pub_date = news_pub_date_tag.text if news_pub_date_tag else None

        news_name_tag = url.find("news:name")
        news_name = news_name_tag.text if news_name_tag else None

        news_language_tag = url.find("news:language")
        news_language = news_language_tag.text if news_language_tag else None

        
        text_content = extract_text_from_url(loc)
        data = {
            "URL": loc,
            "Last Modification": lastmod,
            "News Title": news_title,
            "News Publication Date": news_pub_date,
            "News Name": news_name,
            "News Language": news_language,
            "Text Content": text_content, 
        }

        sitemap_data.append(data)

    return sitemap_data

if __name__ == "__main__":
    sitemap_data_vrt = extract_sitemap_data(sitemap_url_vrt)

    headers = ["URL", "Last Modification", "News Title", "News Publication Date", "News Name", "News Language", "Text Content"]
    with open("sitemap_data_vrt.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(sitemap_data_vrt)
