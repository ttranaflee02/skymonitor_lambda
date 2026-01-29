from minio import Minio
from minio.error import S3Error
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.common.config import Config


def init_minio():
    client = Minio(
        Config.MINIO_ENDPOINT,
        access_key=Config.MINIO_ROOT_USER,
        secret_key=Config.MINIO_ROOT_PASSWORD,
        secure=Config.MINIO_USE_SSL,
    )

    bucket_name = Config.MINIO_BUCKET

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
    except S3Error as err:
        print(f"Error accessing MinIO: {err}")
        sys.exit(1)


if __name__ == "__main__":
    init_minio()
