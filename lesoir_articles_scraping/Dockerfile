FROM python:3.11-slim
RUN mkdir /app
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD [ "python", "lesoir_scraper.py"]