from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

dag = DAG(
    'daily_flight_batch_analytics',
    default_args=default_args,
    description='Daily batch analytics for flight data',
    schedule_interval='0 0 * * *',  # 00:00 daily
    catchup=False
)

run_batch_job = SparkSubmitOperator(
    task_id='run_batch_analytics',
    application='/opt/spark/src/batch_layer/spark_batch_job.py',
    conf={'spark.master': 'spark://spark-master:7077'},
    total_executor_cores=4,
    executor_memory='4g',
    dag=dag
)

run_batch_job