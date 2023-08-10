from datetime import timedelta
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'sivasankari',
    'depends_on_past': False,
    'email': ['sivasankari.mudoms@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scraping_pipeline',
    default_args=default_args,
    schedule_interval='@daily', # Run every day at midnight
    start_date=days_ago(1),
)

dop = DockerOperator(
    docker_url='tcp://docker-proxy:2375',
    image='sivasankari92/scrapper_lavenir_airflow:latest',
    network_mode='bridge',
    task_id='task_scraper_lavenir',
    environment={
        'MONGODB_URL': "{{var.value.mongo_url}}"
    },
    dag=dag,
)

dop2 = DockerOperator(
    docker_url='tcp://docker-proxy:2375',
    image='sivasankari92/scrapper_vrt_airflow1:latest',
    network_mode='bridge',
    task_id='task_scraper_vrt',
    environment={
        'MONGODB_URL': "{{var.value.mongo_url}}"
    },
    dag=dag,
)
dop3 = DockerOperator(
    docker_url='tcp://docker-proxy:2375',
    image='sivasankari92/scrapper_dhnet_airflow:latest',
    network_mode='bridge',
    task_id='task_scraper_dhnet',
    environment={
        'MONGODB_URL': "{{var.value.mongo_url}}"
    },
    dag=dag,
)

dop4 = DockerOperator(
    docker_url='tcp://docker-proxy:2375',
    image='sivasankari92/scrapper_tijd_airflow:latest',
    network_mode='bridge',
    task_id='task_scraper_tijd',
    environment={
        'MONGODB_URL': "{{var.value.mongo_url}}"
    },
    dag=dag,
)
