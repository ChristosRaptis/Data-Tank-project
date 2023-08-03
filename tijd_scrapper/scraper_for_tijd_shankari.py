import requests
from bs4 import BeautifulSoup
import csv
import re

sitemap_url_tijd = "https://www.tijd.be/news/sitemap.xml"

def extract_text_from_url(url):
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'}
    response = requests.get(url,headers=headers)
    

    soup = BeautifulSoup(response.text, "html.parser")
   

    all_paragraphs = soup.find_all('p')
    text_list = []

    for paragraph in all_paragraphs:
        text = paragraph.get_text(strip=True)
        cleaned_text = re.sub(r'[^\x00-\x7F]', '', text)
        text_list.append(cleaned_text)
        
    return "\n".join(text_list)
    

def extract_sitemap_data(sitemap_url):
      
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'}
    response = requests.get(sitemap_url, headers=headers)
   
    
    sitemap_soup = BeautifulSoup(response.text, "lxml")
    print(sitemap_soup)
    urls = sitemap_soup.find_all("url")

    sitemap_data = []
    for url in urls:
        loc = url.find("loc").text
        

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
            "News Title": news_title,
            "News Publication Date": news_pub_date,
            "News Name": news_name,
            "News Language": news_language,
            "News Content": text_content, 
        }

        sitemap_data.append(data)

    return sitemap_data

if __name__ == "__main__":
    sitemap_data_tijd = extract_sitemap_data(sitemap_url_tijd)

    headers = ["URL", "News Title", "News Publication Date", "News Name", "News Language","News Content"]
    with open("sitemap_data_tijd.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(sitemap_data_tijd)
