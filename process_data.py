import os
import pandas as pd

INPUT_FILE = "airsense_data.csv"
OUTPUT_FILE = "airsense_processed.csv"


def process_file():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return False

    df = pd.read_csv(INPUT_FILE)

    df.columns = [
        col.strip().lower().replace(" ", "_").replace("-", "_")
        for col in df.columns
    ]

    df = df.rename(columns={
        "aqi_index": "aqi",
        "pm2_5": "pm25"
    })

    required_cols = [
        "date_ist", "time_ist", "location", "lat", "lon",
        "aqi", "pm25", "pm10", "co", "no2"
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    numeric_cols = ["lat", "lon", "aqi", "pm25", "pm10", "co", "no2"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["aqi", "pm25", "pm10", "co", "no2"]:
        median_val = df[col].median()
        if pd.isna(median_val):
            median_val = 0
        df[col] = df[col].fillna(median_val)

    df["location"] = df["location"].fillna("Unknown")
    df["date_ist"] = df["date_ist"].fillna("Not Available")
    df["time_ist"] = df["time_ist"].fillna("Not Available")

    final_cols = [
        "date_ist", "time_ist", "location", "lat", "lon",
        "aqi", "pm25", "pm10", "co", "no2"
    ]

    processed = df[final_cols].copy()
    processed.to_csv(OUTPUT_FILE, index=False)

    print("Processed file created successfully.")
    print(processed.head())
    return True


if __name__ == "__main__":
    process_file()