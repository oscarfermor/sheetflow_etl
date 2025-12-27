import os
from dotenv import load_dotenv


load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SHEET_ID = os.getenv("GOOGLE_SHEETS_ID", "")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME", "")
AWS_S3_RAW_PREFIX = os.getenv("AWS_S3_RAW_PREFIX", "")
DATE_FORMAT = "%Y-%m-%dT%H-%M-%S"
