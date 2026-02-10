# SheetFlow ETL

Built an automated data pipeline that extracts Google Sheets data via a Google Service Account, processes and stores cleansed parquet files in AWS S3, and makes the data queryable in Databricks for SQL analytics and dashboards.

## 🚀 Project Overview

SheetFlow ETL connects to one or more Google Sheets, extracts data programmatically using a service account, and standardizes it for automated processing. This is ideal for scheduled ETL jobs and data pipelines.
<img width="1659" height="988" alt="image" src="https://github.com/user-attachments/assets/70df1a5f-4303-48b4-9178-7d95531b2c70" />

## 🧠 Prerequisites

Before running the pipeline, you need:

- Python 3.8+
- A Google Cloud project
- A Google Sheet you want to access
- A Google Service Account authorized to read the sheet

### Databricks Prerequisites (Optional)

To export data to Databricks:

- A Databricks workspace
- A Databricks personal access token (generated in **User Settings > Developer > Access Tokens**)
- Databricks workspace URL (e.g., `https://dbc-xxxxx.cloud.databricks.com`)
- A target catalog and schema where tables will be created

#### S3 to Databricks Connection Steps

1. **Configure S3 credentials in Databricks:**
   - In Databricks workspace, go to **Catalog > External Locations**.
   - Create a new external location pointing to your S3 bucket.
   - Provide your AWS access key and secret access key.
   - Set the path to your S3 bucket (e.g., `s3://your-bucket/google_sheets/`).

2. **Create a storage credential (if not using external location):**
   - Go to **Admin Console > Credentials** in Databricks.
   - Click **Create Storage Credential**.
   - Select **AWS S3** and provide your AWS credentials.
   - Name it (e.g., `s3-sheetflow-credential`).

3. **Configure IAM role (recommended for production):**
   - In AWS, create an IAM role with S3 bucket access permissions.
   - In Databricks, use the role ARN instead of access keys for better security.

4. **Load Parquet files from S3 to Databricks:**
   - Use Spark SQL or Python API to load Parquet files:
     ```python
     spark.read.parquet("s3://your-bucket/google_sheets/").write.mode("overwrite").option("path", "dbfs:/path/to/table").saveAsTable("your_table")
     ```

## 📌 Step 1 — Enable Google APIs

1. Go to the **Google Cloud Console**.
2. Navigate to **APIs & Services > Library**.
3. Search for **Google Sheets API** and click **Enable**.
4. Search for **Google Drive API** and click **Enable**.  
   Both APIs are required for full access to Sheets metadata.:contentReference[oaicite:0]{index=0}

## 🔑 Step 2 — Create a Service Account

1. In the Google Cloud Console, go to **IAM & Admin > Service Accounts**.
2. Click **Create Service Account**.
3. Give it a name (e.g., `sheetflow-etl-sa`) and click **Create**.
4. Skip assigning roles (for basic access a Viewer role is fine).
5. Go to the new service account’s **Keys** tab.
6. Click **Add Key > Create new key**.
7. Choose **JSON** and click **Create**.
8. Save the downloaded JSON file (e.g., `credentials.json`).  
   This file contains the credentials your script needs.:contentReference[oaicite:1]{index=1}

## 📄 Step 3 — Share the Google Sheet with the Service Account

1. Open your Google Sheet in the browser.
2. Click the **Share** button.
3. Paste the **service account email** from the JSON file (the `client_email` field).
4. Give at least **Viewer** access (Editor if you need write access).
5. Click **Send**.  
   Service accounts must be explicitly granted access to each sheet they interact with.:contentReference[oaicite:2]{index=2}

## 🧾 Step 4 — Install Python Dependencies

```bash
pip install -r requirements.txt
```

## ⚙️ Configuration

- Copy `env.example` to a local `.env` file and edit the values:
   - `GOOGLE_SHEETS_ID` — the Google Sheets *spreadsheet ID* (the long id in the sheet URL between `/d/` and `/edit`).
   - `GOOGLE_CREDENTIALS_PATH` — path to your Service Account JSON (for example: `config/credentials.json`).
   - (Optional) `AWS_BUCKET_NAME`, `AWS_REGION_NAME`, `AWS_S3_RAW_PREFIX` — used when uploading raw files to S3.

The project uses `python-dotenv` to load `.env` automatically (see `src/constants.py`).

## ▶️ Running the ETL

- Run the orchestrator script to perform extraction (and upload if enabled):

```bash
python src/orchestator.py
```

Note: the orchestrator reads `GOOGLE_SHEETS_ID` and `GOOGLE_CREDENTIALS_PATH` from the environment and uses `SheetsExtractor` to pull all worksheets into pandas DataFrames. S3 uploading is implemented in `src/s3_client.py` (example calls are in `src/orchestator.py` and currently commented out).

## ☁️ S3 Uploads

The repository provides a small `S3Client` wrapper (`src/s3_client.py`) with `upload_file`, `upload_bytes` and `list_objects`. There is also a helper `make_s3_key(sheet_id, worksheet)` in `src/utils.py` which generates a timestamped, partitioned key like:

```
google_sheets/{sheet_id}/{worksheet}/year=YYYY/month=MM/day=DD/{sheet_id}_{worksheet}_{timestamp}.csv
```

To enable uploads:

1. Set `AWS_BUCKET_NAME` and `AWS_REGION_NAME` in your `.env` (or provide credentials via the usual AWS env vars / config).
2. Instantiate `S3Client(bucket_name, region_name)` and call `upload_file` or `upload_bytes` with a suitable key.

## 🗄️ Databricks Export

SheetFlow ETL can export extracted data directly to Databricks as Delta tables. This enables seamless integration with Databricks lakehouses for further analytics and reporting.

### Results
<img width="1888" height="1278" alt="Screenshot 2026-02-10 at 10 23 18" src="https://github.com/user-attachments/assets/cd59249b-1745-4324-bb60-18ffd8426915" />

<img width="2294" height="1269" alt="Screenshot 2026-02-10 at 10 23 32" src="https://github.com/user-attachments/assets/780f022e-2bf5-4c51-a958-ddfc58ffd17e" />


## ✅ Tests

Run the test suite with `pytest` from the repository root:

```bash
pytest
```

Tests live under `src/tests` and mock external services where possible.

---
If you'd like, I can also add a short example `docker-compose` service or a small CLI wrapper to make running the orchestrator and enabling S3 uploads more straightforward. 🔧
