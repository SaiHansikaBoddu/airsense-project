import urllib.parse
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for
from sqlalchemy import create_engine, text

app = Flask(__name__)

# Azure SQL details
server = "airsense-db-2026-01.database.windows.net"
database = "airsense-db"
username = "airsenseadmin"
password = "HANSIKA@001"

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True,
    pool_pre_ping=True,
    pool_recycle=300
)


def get_aqi_status(aqi):
    if aqi <= 50:
        return "Good", "#19c37d", "Air quality is safe. Enjoy your day outdoors!"
    elif aqi <= 100:
        return "Moderate", "#f4b400", "Moderate pollution detected. Sensitive groups should reduce outdoor exercise."
    elif aqi <= 200:
        return "Poor", "#f39c12", "Poor air quality. Limit long outdoor exposure."
    else:
        return "Hazardous", "#e74c3c", "Hazardous air quality. Stay indoors if possible."


@app.route("/")
def home():
    selected_city = request.args.get("city", "").strip()

    try:
        engine.dispose()

        cities_df = pd.read_sql(
            text("""
                SELECT location, AVG(lat) AS lat, AVG(lon) AS lon, AVG(aqi) AS aqi
                FROM aqi_data
                GROUP BY location
            """),
            engine
        )
    except Exception as e:
        return f"Database connection error: {e}"

    if cities_df.empty:
        return "No data found in database. Please upload a CSV file."

    cities = sorted(cities_df["location"].dropna().unique().tolist())

    if not selected_city or selected_city not in cities:
        if "Hyderabad" in cities:
            selected_city = "Hyderabad"
        else:
            selected_city = cities[0]

    try:
        df = pd.read_sql(
            text("SELECT * FROM aqi_data WHERE location = :city"),
            engine,
            params={"city": selected_city}
        )
    except Exception as e:
        return f"City data loading error: {e}"

    if df.empty:
        return f"No data found for {selected_city}"

    avg_aqi = round(df["aqi"].mean(), 2)
    pm25 = round(df["pm25"].mean(), 2)
    pm10 = round(df["pm10"].mean(), 2)
    no2 = round(df["no2"].mean(), 2)
    co = round(df["co"].mean(), 2)

    aqi_status, aqi_color, aqi_message = get_aqi_status(avg_aqi)

    center_lat = float(df["lat"].mean())
    center_lon = float(df["lon"].mean())
    zoom_level = 11

    trend_labels = ["02:00", "06:00", "10:00", "14:00", "18:00", "23:00"]

    trend_aqi = [
        max(avg_aqi - 5, 0),
        max(avg_aqi - 3, 0),
        avg_aqi,
        avg_aqi + 2,
        avg_aqi + 1,
        max(avg_aqi - 1, 0),
    ]

    trend_pm25 = [
        max(pm25 - 2, 0),
        max(pm25 - 1, 0),
        pm25,
        pm25 + 1,
        pm25 + 2,
        pm25,
    ]

    pollutant_labels = ["PM2.5", "PM10", "NO2", "CO"]
    pollutant_values = [pm25, pm10, no2, co]

    map_data = cities_df.to_dict(orient="records")

    return render_template(
        "index.html",
        cities=cities,
        selected_city=selected_city,
        avg_aqi=avg_aqi,
        aqi_status=aqi_status,
        aqi_color=aqi_color,
        aqi_message=aqi_message,
        pm25=pm25,
        pm10=pm10,
        no2=no2,
        co=co,
        map_data=map_data,
        center_lat=center_lat,
        center_lon=center_lon,
        zoom_level=zoom_level,
        trend_labels=trend_labels,
        trend_aqi=trend_aqi,
        trend_pm25=trend_pm25,
        pollutant_labels=pollutant_labels,
        pollutant_values=pollutant_values
    )


@app.route("/upload", methods=["POST"])
def upload():
    file = request.files.get("file")

    if not file or file.filename == "":
        return "No file uploaded"

    df = None

    for enc in ["utf-8", "utf-8-sig", "latin1", "cp1252"]:
        try:
            file.stream.seek(0)
            df = pd.read_csv(file, encoding=enc)
            break
        except UnicodeDecodeError:
            continue

    if df is None:
        return "Unable to read CSV file. Please save it as UTF-8 CSV and try again."

    df.columns = [col.strip().lower() for col in df.columns]

    required_cols = ["location", "lat", "lon", "aqi", "pm25", "pm10", "no2", "co"]

    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        return f"Missing columns in CSV: {', '.join(missing)}"

    df = df[required_cols]

    numeric_cols = ["lat", "lon", "aqi", "pm25", "pm10", "no2", "co"]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["location"] = df["location"].astype(str).str.strip()

    df = df.dropna(subset=required_cols)

    # Limit rows for fast upload
    df = df.head(300)

    if df.empty:
        return "Uploaded CSV has no valid rows after cleaning."

    try:
        engine.dispose()

        with engine.begin() as conn:
            conn.execute(text("DELETE FROM aqi_data"))

        df.to_sql(
            "aqi_data",
            engine,
            if_exists="append",
            index=False,
            chunksize=50
        )

    except Exception as e:
        return f"Upload/database error: {e}"

    first_city = df["location"].iloc[0]

    return redirect(url_for("home", city=first_city))


if __name__ == "__main__":
    app.run(debug=True)