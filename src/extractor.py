import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from typing import List
from logger import logger


class SheetsExtractor:
    def __init__(self, credentials_file: str, spreadsheet_id: str, scopes: List[str]):
        """
        Args:
        credentials_file: Path to the Google Service Account JSON
        spreadsheet_id: Google Sheets spreadsheet ID
        """
        self.credentials_file = credentials_file
        self.spreadsheet_id = spreadsheet_id
        self.scopes = scopes
        self.client = None

    def connect(self) -> None:
        try:
            creds = Credentials.from_service_account_file(
                self.credentials_file, scopes=self.scopes
            )
            self.client = gspread.authorize(creds)
            logger.info("Google Sheets client authorized successfully.")
        except Exception as e:
            logger.error(f"Failed to authorize Google Sheets client: {e}")

    def extract_all(self) -> dict[str, pd.DataFrame]:
        if self.client is None:
            logger.error("Google Sheets client not initialized.")
            return pd.DataFrame()

        if not self.spreadsheet_id or not isinstance(self.spreadsheet_id, str):
            logger.error("Invalid spreadsheet_id: must be a non-empty string.")
            return pd.DataFrame()

        try:
            ss = self.client.open_by_key(self.spreadsheet_id)
            data = {}

            for ws in ss.worksheets():
                df = pd.DataFrame(ws.get_all_records())
                data[ws.title] = df

            logger.info(
                f"Extracted records from in spreadsheet '{self.spreadsheet_id}'."
            )
            return data

        except Exception as e:
            logger.error(
                f"Error extracting data from in spreadsheet '{self.spreadsheet_id}': {e}"
            )
            return pd.DataFrame()
