from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import os

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
    catchup=False,
    user_defined_macros={
        'spark_master': os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
    }
)

# Get the current date for batch processing
def get_batch_date(**context):
    execution_date = context['logical_date']
    return execution_date.strftime('%Y-%m-%d')

# Task to determine the batch date
set_batch_date = PythonOperator(
    task_id='set_batch_date',
    python_callable=get_batch_date,
    dag=dag
)

# Spark batch job
run_batch_job = SparkSubmitOperator(
    task_id='run_batch_analytics',
    application='/opt/spark/src/batch_layer/spark_batch_job.py',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    },
    total_executor_cores=4,
    executor_memory='4g',
    jars='/opt/spark/jars/elasticsearch-spark-connector_2.12-8.10.2.jar,/opt/spark/jars/mongo-spark-connector_2.12-3.0.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar',
    dag=dag
)

# Set task dependencies
set_batch_date >> run_batch_job