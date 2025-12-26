import os
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import logging

# Set up logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# Load variables from .env file
load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SHEET_ID = os.getenv("GOOGLE_SHEETS_ID")
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")

try:
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    client = gspread.authorize(creds)
    logger.info("Google Sheets client authorized successfully.")
except Exception as e:
    logger.error(f"Failed to authorize Google Sheets client: {e}")
    creds = None
    client = None


def extract_sheet(spreadsheet_id, worksheet):
    if client is None:
        logger.error("Google Sheets client not initialized.")
        return pd.DataFrame()
    if not spreadsheet_id or not isinstance(spreadsheet_id, str):
        logger.error("Invalid spreadsheet_id: must be a non-empty string.")
        return pd.DataFrame()
    if not worksheet or not isinstance(worksheet, str):
        logger.error("Invalid worksheet: must be a non-empty string.")
        return pd.DataFrame()
    try:
        sheet = client.open_by_key(spreadsheet_id)
        ws = sheet.worksheet(worksheet)
        records = ws.get_all_records()
        logger.info(f"Extracted {len(records)} records from worksheet '{worksheet}' in spreadsheet '{spreadsheet_id}'.")
        return pd.DataFrame(records)
    except Exception as e:
        logger.error(f"Error extracting data from worksheet '{worksheet}' in spreadsheet '{spreadsheet_id}': {e}")
        return pd.DataFrame()


def main():
    if not SHEET_ID:
        logger.error("Missing SHEET_ID in environment variables.")
        return
    df = extract_sheet(SHEET_ID, "test1")
    logger.info(f"Extracted dataframe shape: {df.shape}")
    print(df)


if __name__ == "__main__":
    main()
