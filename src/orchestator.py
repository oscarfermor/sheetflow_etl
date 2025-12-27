import os
from logger import logger
from extractor import SheetsExtractor

# from raw_data_landing import write_raw


SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SHEET_ID = os.getenv("GOOGLE_SHEETS_ID")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")


def main():
    print("Starting ETL run...")

    # 1) Extract
    if not SHEET_ID:
        logger.error("Missing SHEET_ID in environment variables.")
        return
    extractor = SheetsExtractor(credentials_file=CREDENTIALS_PATH, spreadsheet_id=SHEET_ID, scopes=SCOPES)
    extractor.connect()
    data = extractor.extract_all()
    logger.info(data)

    # 2) Write raw
    # write_raw(sheet_data)

    print("ETL run completed!")


if __name__ == "__main__":
    main()
