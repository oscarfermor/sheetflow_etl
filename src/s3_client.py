import boto3
from botocore.exceptions import ClientError
from io import StringIO
from datetime import datetime
from logger import logger

s3 = boto3.client("s3")


class S3Client:
    def __init__(self, bucket_name: str, region_name: str):
        """
        AWS S3 client wrapper.

        Args:
            bucket_name: Target S3 bucket name
            region_name: Optional AWS region
        """
        self.bucket_name = bucket_name
        self.s3 = boto3.client("s3", region_name=region_name)

    def upload_file(self, local_path: str, key: str) -> bool:
        """
        Upload a local file to S3.

        Args:
            local_path: Path to local file to upload
            key: S3 object key (folder/path + file name)

        Returns:
            True if uploaded successfully, False otherwise
        """
        try:
            self.s3.upload_file(local_path, self.bucket_name, key)
            logger.info(f"Uploaded file to s3://{self.bucket_name}/{key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            return False

    def upload_bytes(self, data: bytes, key: str) -> bool:
        """
        Upload raw bytes or CSV content to S3.

        Args:
            data: Content bytes
            key: S3 object key (folder/path + file name)
        """
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=data)
            logger.info(f"Uploaded object to s3://{self.bucket_name}/{key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload data to {key}: {e}")
            return False

    def list_objects(self, prefix: str = None):
        """
        List objects under the given S3 prefix.

        Args:
            prefix: Filter by prefix (folder)
        """
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            return response.get("Contents", [])
        except ClientError as e:
            logger.error(f"Failed to list objects under {prefix}: {e}")
            return []
