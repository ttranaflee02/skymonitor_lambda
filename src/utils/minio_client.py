from minio import Minio
from minio.error import S3Error
from datetime import datetime

class MinIOHelper:
    def __init__(self, config):
        self.config = config
        self.client = Minio(
            config.MINIO_ENDPOINT,
            access_key=config.MINIO_ROOT_USER,
            secret_key=config.MINIO_ROOT_PASSWORD,
            secure=config.MINIO_USE_SSL
        )
    
    def get_date_path(self, date_str: str) -> str:
        """Convert YYYY-MM-DD to year/month/day path."""
        parts = date_str.split("-")
        return f"{parts[0]}/{parts[1]}/{parts[2]}"
    
    def list_objects_by_date(self, date_str: str):
        """List all objects for a given date."""
        prefix = self.get_date_path(date_str)
        return self.client.list_objects(self.config.MINIO_BUCKET, prefix=prefix)

    def upload_json(self, object_name: str, data: dict):
        """Upload a dictionary as JSON to MinIO."""
        import json
        from io import BytesIO
        
        data_bytes = json.dumps(data).encode('utf-8')
        self.client.put_object(
            self.config.MINIO_BUCKET,
            object_name,
            BytesIO(data_bytes),
            length=len(data_bytes),
            content_type='application/json'
        )