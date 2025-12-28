# utils/key_generator.py
from datetime import datetime, timezone


def make_s3_key_bronze(prefix: str, sheet_id: str, worksheet: str) -> str:
    now = datetime.now(timezone.utc)
    clean_ws = worksheet.lower().replace(" ", "_")
    timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")

    return (
        f"{prefix}{sheet_id}/{clean_ws}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{sheet_id}_{clean_ws}_{timestamp}"
    )


def make_s3_key_silver(prefix: str, sheet_id: str, worksheet: str) -> str:
    now = datetime.now(timezone.utc)
    clean_ws = worksheet.lower().replace(" ", "_")
    timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")

    return (
        f"{prefix}"
        f"sheet_id={sheet_id}/"
        f"load_date={now.strftime('%Y-%m-%d')}/"
        f"{sheet_id}_{clean_ws}_{timestamp}"
    )
