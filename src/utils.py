# utils/key_generator.py
from datetime import datetime, timezone


def make_s3_key_bronze(prefix: str, sheet_id: str, worksheet: str) -> str:
    now = datetime.now(timezone.utc)
    clean_ws = worksheet.lower().replace(" ", "_")
    timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")

    return (
        f"{prefix}"
        f"sheet_id={sheet_id}/"
        f"worksheet={clean_ws}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{timestamp}.csv"
    )


def make_s3_key_silver(prefix: str, sheet_id: str, worksheet: str) -> str:
    now = datetime.now(timezone.utc)
    clean_ws = worksheet.lower().replace(" ", "_")
    timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")

    return (
        f"{prefix}"
        f"sheet_id={sheet_id}/"
        f"worksheet={clean_ws}/"
        f"load_date={now.strftime('%Y-%m-%d')}/"
        f"{timestamp}.parquet"
    )
