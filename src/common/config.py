import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # OpenSky API
    OPENSKY_USERNAME = os.getenv("OPENSKY_USERNAME", "")
    OPENSKY_PASSWORD = os.getenv("OPENSKY_PASSWORD", "")
    OPENSKY_API_URL = "https://opensky-network.org/api/states/all"
    POLL_INTERVAL_SECONDS = 15

    # Kafka
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
    KAFKA_TOPIC_FLIGHTS = os.getenv("KAFKA_TOPIC_FLIGHTS", "flight-live")

    # MinIO
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
    MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "flight-data")
    MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "False").lower() == "true"

    # MongoDB
    MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    MONGODB_DB = os.getenv("MONGODB_DB", "skymonitor")
    MONGODB_COLLECTION_FLIGHTS = "flights_realtime"

    # Elasticsearch
    ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
    ELASTICSEARCH_PORT = int(os.getenv("ELASTICSEARCH_PORT", "9200"))
    ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "flight-analytics")

    # Spark
    SPARK_APP_NAME_BATCH = "FlightBatchAnalytics"
    SPARK_APP_NAME_STREAMING = "FlightStreamingProcessor"

    # Business Logic / Geo
    VIETNAM_LAT_MIN = float(os.getenv("VIETNAM_LAT_MIN", "8.5"))
    VIETNAM_LAT_MAX = float(os.getenv("VIETNAM_LAT_MAX", "23.5"))
    VIETNAM_LON_MIN = float(os.getenv("VIETNAM_LON_MIN", "102.0"))
    VIETNAM_LON_MAX = float(os.getenv("VIETNAM_LON_MAX", "109.5"))
    
    RAPID_DESCENT_THRESHOLD = -5.0  # m/s
    MIN_ALTITUDE_FOR_DESCENT = 500  # meters
    TRIGGER_INTERVAL_SECONDS = 30

    # System
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
