import pytest
import pandas as pd
from unittest import mock
from src import extract


@pytest.fixture(autouse=True)
def reload_env(monkeypatch):
    monkeypatch.setenv("GOOGLE_SHEETS_ID", "dummy_id")
    monkeypatch.setenv("GOOGLE_CREDENTIALS_PATH", "dummy_credentials.json")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@mock.patch("src.extract.gspread")
@mock.patch("src.extract.Credentials")
def test_extract_sheet_success(mock_creds, mock_gspread):
    # Setup mock client
    mock_client = mock.Mock()
    mock_ws = mock.Mock()
    mock_ws.get_all_records.return_value = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    mock_client.open_by_key.return_value.worksheet.return_value = mock_ws
    mock_gspread.authorize.return_value = mock_client
    mock_creds.from_service_account_file.return_value = object()

    with mock.patch("src.extract.client", mock_client):
        df = extract.extract_sheet("dummy_id", "dummy_worksheet")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2


@mock.patch("src.extract.client", None)
def test_extract_sheet_no_client():
    df = extract.extract_sheet("any", "worksheet")
    assert df.empty


@mock.patch("src.extract.gspread")
def test_extract_sheet_invalid_input(mock_gspread):
    # Valid client but invalid sheet id and worksheet
    with mock.patch("src.extract.client", mock.Mock()):
        # Invalid spreadsheet_id
        df = extract.extract_sheet(None, "ws")
        assert df.empty
        df = extract.extract_sheet("", "ws")
        assert df.empty
        # Invalid worksheet
        df = extract.extract_sheet("id", None)
        assert df.empty
        df = extract.extract_sheet("id", 123)
        assert df.empty
