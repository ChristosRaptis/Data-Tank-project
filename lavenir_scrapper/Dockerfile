FROM python:3.11
RUN mkdir /app
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD [ "python", "scrapper_lavenir_db_airflow.py"]
