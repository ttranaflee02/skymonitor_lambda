from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
from kafka import KafkaProducer

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'start_date': days_ago(1),
}

dag = DAG(
    'health_check_pipeline',
    default_args=default_args,
    description='Health checks for Kafka and OpenSky API',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False
)

def check_opensky_api():
    try:
        response = requests.get('https://opensky-network.org/api/states/all', timeout=10)
        if response.status_code == 200:
            print("✓ OpenSky API is healthy")
        else:
            raise Exception(f"OpenSky returned {response.status_code}")
    except Exception as e:
        print(f"✗ OpenSky API check failed: {e}")
        raise

def check_kafka():
    try:
        producer = KafkaProducer(bootstrap_servers='kafka:9092', request_timeout_ms=5000)
        producer.close()
        print("✓ Kafka is healthy")
    except Exception as e:
        print(f"✗ Kafka check failed: {e}")
        raise

check_api_task = PythonOperator(
    task_id='check_opensky_api',
    python_callable=check_opensky_api,
    dag=dag
)

check_kafka_task = PythonOperator(
    task_id='check_kafka',
    python_callable=check_kafka,
    dag=dag
)

[check_api_task, check_kafka_task]