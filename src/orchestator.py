import io
from logger import logger
from extractor import SheetsExtractor
from constants import (
    SHEET_ID,
    CREDENTIALS_PATH,
    SCOPES,
    AWS_BUCKET_NAME,
    AWS_REGION_NAME,
    AWS_S3_RAW_PREFIX,
)
from s3_client import S3Client
from utils import make_s3_key

# from raw_data_landing import write_raw


def main():
    print("Starting ETL run...")

    # 1) Extract
    if not SHEET_ID:
        logger.error("Missing SHEET_ID in environment variables.")
        return
    extractor = SheetsExtractor(
        credentials_file=CREDENTIALS_PATH, spreadsheet_id=SHEET_ID, scopes=SCOPES
    )
    extractor.connect()
    data = extractor.extract_all()
    if not data:
        logger.error("No data extracted from Google Sheets.")
        return
    # 2) Write raw
    s3Client = S3Client(bucket_name=AWS_BUCKET_NAME, region_name=AWS_REGION_NAME)
    for worksheet, df in data.items():
        s3_key = (
            make_s3_key(
                prefix=AWS_S3_RAW_PREFIX, sheet_id=SHEET_ID, worksheet=worksheet
            )
            + ".csv"
        )
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        s3Client.upload_bytes(data=buffer.getvalue(), s3_key=s3_key)

    print("ETL run completed!")


if __name__ == "__main__":
    main()
