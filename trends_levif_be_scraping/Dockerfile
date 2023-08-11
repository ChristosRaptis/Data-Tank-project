# FROM python:3.8-slim

# WORKDIR /app
# RUN apt-get update && apt-get install -y libxml2-dev libxslt1-dev
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt
# COPY trends_levif_scraper.py .
# CMD ["python", "trends_levif_scraper.py"]

FROM python:3.11
RUN mkdir /app
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD [ "python", "trends_levif_scraper_mongo.py"]