from mlb_hits_fetcher import fetch_all_hits, process_hit_data, TEAMS
from upload import upload_to_bigquery
import logging

def main():
    season = 2024

    logging.info("Starting MLB hit data fetch...")
    df_raw = fetch_all_hits(TEAMS, season)
    logging.info(f"Total raw records fetched: {len(df_raw)}")

    df_clean = process_hit_data(df_raw)
    logging.info(f"Filtered records with coordinates: {len(df_clean)}")

    df_clean.to_csv("mlb_hit_data_2024.csv", index=False)
    logging.info(" Saved CSV to mlb_hit_data_2024.csv")

    upload_to_bigquery(df_clean)

if __name__ == "__main__":
    main()
