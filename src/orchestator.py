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
    AWS_S3_SILVER_PREFIX,
)
from s3_client import S3Client
from utils import make_s3_key_bronze, make_s3_key_silver
from transformer import BronzeToSilverTransformer

# from raw_data_landing import write_raw


def store_bronze_data():
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
            make_s3_key_bronze(
                prefix=AWS_S3_RAW_PREFIX, sheet_id=SHEET_ID, worksheet=worksheet
            )
            + ".csv"
        )
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        s3Client.upload_bytes(data=buffer.getvalue(), s3_key=s3_key)
    return data


def transform_to_silver(bronze_data):
    if not bronze_data:
        logger.error("No bronze data to transform.")
        return

    transformer = BronzeToSilverTransformer()
    s3Client = S3Client(bucket_name=AWS_BUCKET_NAME, region_name=AWS_REGION_NAME)
    for worksheet, df in bronze_data.items():
        # Transform
        df_silver = transformer.transform(df, worksheet)

        # Store in silver layer
        s3_key = (
            make_s3_key_silver(
                prefix=AWS_S3_SILVER_PREFIX, sheet_id=SHEET_ID, worksheet=worksheet
            )
            + ".parquet"
        )
        buffer = io.BytesIO()
        df_silver.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        s3Client.upload_bytes(data=buffer.getvalue(), s3_key=s3_key)

        logger.info(f"Stored silver data: {s3_key}")
        logger.debug(
            f"Transformations applied: {transformer.get_transformation_summary()}"
        )


def main():
    logger.info("Starting ETL run...")

    bronze_data = store_bronze_data()

    if bronze_data:
        transform_to_silver(bronze_data)

    logger.info("ETL run completed!")


if __name__ == "__main__":
    main()
