import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Any, Optional
from .logger import logger


class S3Client:
    def __init__(self, bucket_name: str, region_name: str):
        """
        AWS S3 client wrapper.

        Args:
            bucket_name: Target S3 bucket name
            region_name: AWS region
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
        except FileNotFoundError as e:
            logger.error(f"Local file not found: {local_path}")
            return False

    def upload_bytes(self, data: bytes, key: str) -> bool:
        """
        Upload raw bytes to S3.

        Args:
            data: Content bytes
            key: S3 object key (folder/path + file name)

        Returns:
            True if uploaded successfully, False otherwise
        """
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=data)
            logger.info(f"Uploaded object to s3://{self.bucket_name}/{key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload data to {key}: {e}")
            return False

    def list_objects(self, prefix: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all objects under the given S3 prefix (with pagination).

        Args:
            prefix: Filter by prefix (folder). If None, lists all objects.

        Returns:
            List of object metadata dictionaries
        """
        try:
            objects = []
            paginator = self.s3.get_paginator("list_objects_v2")

            page_params = {"Bucket": self.bucket_name}
            if prefix:
                page_params["Prefix"] = prefix

            for page in paginator.paginate(**page_params):
                objects.extend(page.get("Contents", []))

            return objects
        except ClientError as e:
            logger.error(f"Failed to list objects under {prefix}: {e}")
            return []
