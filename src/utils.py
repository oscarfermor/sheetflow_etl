# utils/key_generator.py
from datetime import datetime, timezone


def make_s3_key(sheet_id: str, worksheet: str) -> str:
    now = datetime.now(timezone.utc)
    clean_ws = worksheet.lower().replace(" ", "_")
    timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")

    return (
        f"google_sheets/{sheet_id}/{clean_ws}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{sheet_id}_{clean_ws}_{timestamp}.csv"
    )
