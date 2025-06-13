import os
import logging
import pandas as pd
from google.oauth2 import service_account
from pandas_gbq import to_gbq

def upload_to_bigquery(df: pd.DataFrame):
    project_id = os.getenv("GCP_PROJECT_ID")
    table_id = os.getenv("BQ_TABLE_ID")
    key_file = "gcp_key.json"  # written by GitHub secret in workflow

    credentials = service_account.Credentials.from_service_account_file(key_file)

    logging.info(f"Uploading {len(df)} rows to BigQuery: {table_id}")
    to_gbq(df, table_id, project_id=project_id, if_exists="replace", credentials=credentials)
    logging.info("Upload complete.")
