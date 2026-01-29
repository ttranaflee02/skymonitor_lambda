import sys
import os
import logging
from datetime import datetime

# Add src to path
sys.path.append(os.getcwd())

from src.common.config import Config
from src.utils.minio_client import MinIOHelper

# Setup basic logging to see output
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def verify_minio_write():
    print(f"Connecting to MinIO at {Config.MINIO_ENDPOINT}...")
    try:
        helper = MinIOHelper(Config)

        # 1. Check if bucket exists
        exists = helper.client.bucket_exists(Config.MINIO_BUCKET)
        print(f"Bucket '{Config.MINIO_BUCKET}' exists: {exists}")

        if not exists:
            print("Bucket does not exist. Attempting to create...")
            helper.client.make_bucket(Config.MINIO_BUCKET)
            print("Bucket created.")

        # 2. Try to list objects
        print("Listing objects...")
        objects = helper.list_objects_by_date(datetime.now().strftime("%Y-%m-%d"))
        obj_list = list(objects)
        print(f"Found {len(obj_list)} objects for today.")

        # 3. Try to upload a test file
        test_file_name = f"test_write_{datetime.now().strftime('%H%M%S')}.json"
        print(f"Attempting to upload {test_file_name}...")
        helper.upload_json(
            test_file_name, {"test": "data", "timestamp": str(datetime.now())}
        )
        print("Upload successful!")

        # 4. Cleanup
        # helper.client.remove_object(Config.MINIO_BUCKET, test_file_name)
        # print("Test object removed.")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    verify_minio_write()
