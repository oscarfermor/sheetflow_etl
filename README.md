# SheetFlow ETL

A Python ETL pipeline that extracts data from Google Sheets using a Google Service Account, then stores it for further transformation and loading (AWS-ready).

## 🚀 Project Overview

SheetFlow ETL connects to one or more Google Sheets, extracts data programmatically using a service account, and standardizes it for automated processing. This is ideal for scheduled ETL jobs and data pipelines.

## 🧠 Prerequisites

Before running the pipeline, you need:

- Python 3.8+
- A Google Cloud project
- A Google Sheet you want to access
- A Google Service Account authorized to read the sheet

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
pip install gspread google-auth pandas
```
