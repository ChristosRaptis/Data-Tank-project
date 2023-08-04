FROM python:3.8-slim

WORKDIR /app
RUN apt-get update && apt-get install -y libxml2-dev libxslt1-dev
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY trends_levif_scraper.py .
CMD ["python", "trends_levif_scraper.py"]