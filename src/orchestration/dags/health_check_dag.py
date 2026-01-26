from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import os

# Try to import kafka, but handle the case where it's not available
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': days_ago(1),
}

dag = DAG(
    'health_check_pipeline',
    default_args=default_args,
    description='Health checks for Kafka and OpenSky API',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False
)

def check_opensky_api(**context):
    """Check if OpenSky API is accessible."""
    try:
        response = requests.get('https://opensky-network.org/api/states/all', timeout=10)
        if response.status_code == 200:
            context['ti'].log.info("✓ OpenSky API is healthy")
            context['ti'].xcom_push(key='opensky_status', value='healthy')
            return "healthy"
        else:
            raise Exception(f"OpenSky returned {response.status_code}")
    except Exception as e:
        context['ti'].log.error(f"✗ OpenSky API check failed: {e}")
        context['ti'].xcom_push(key='opensky_status', value='unhealthy')
        raise

def check_kafka(**context):
    """Check if Kafka is accessible from within the Docker network."""
    if not KAFKA_AVAILABLE:
        context['ti'].log.warning("Kafka Python client not installed, skipping Kafka check")
        context['ti'].xcom_push(key='kafka_status', value='skipped')
        return "skipped"

    kafka_brokers = os.getenv('KAFKA_BROKERS', 'kafka:9092')
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_brokers,
            request_timeout_ms=5000,
            security_protocol='PLAINTEXT'  # Use PLAINTEXT for Docker network
        )
        producer.close()
        context['ti'].log.info(f"✓ Kafka is healthy at {kafka_brokers}")
        context['ti'].xcom_push(key='kafka_status', value='healthy')
        return "healthy"
    except Exception as e:
        context['ti'].log.error(f"✗ Kafka check failed: {e}")
        context['ti'].xcom_push(key='kafka_status', value='unhealthy')
        raise

def check_mongo(**context):
    """Check if MongoDB is accessible."""
    try:
        from pymongo import MongoClient
        mongo_uri = os.getenv('MONGODB_URI', 'mongodb://mongo:27017')
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        client.close()
        context['ti'].log.info(f"✓ MongoDB is healthy at {mongo_uri}")
        context['ti'].xcom_push(key='mongo_status', value='healthy')
        return "healthy"
    except Exception as e:
        context['ti'].log.error(f"✗ MongoDB check failed: {e}")
        context['ti'].xcom_push(key='mongo_status', value='unhealthy')
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

check_mongo_task = PythonOperator(
    task_id='check_mongo',
    python_callable=check_mongo,
    dag=dag
)

# All health checks run in parallel
[check_api_task, check_kafka_task, check_mongo_task]