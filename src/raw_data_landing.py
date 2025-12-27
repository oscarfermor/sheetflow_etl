import boto3
from io import StringIO
from datetime import datetime

s3 = boto3.client("s3")


def write_raw_to_s3(df, source, bucket):
    date = datetime.timezone.utc.strftime("%Y-%m-%d_%H%M%S")
    key = f"raw/google_sheets/{source}/{date}.csv"
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
