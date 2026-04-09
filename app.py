import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for
from werkzeug.utils import secure_filename
from process_data import process_file

app = Flask(__name__)

RAW_FILE = "airsense_data.csv"
PROCESSED_FILE = "airsense_processed.csv"
ALLOWED_EXTENSIONS = {"csv"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def get_aqi_status(aqi):
    if aqi <= 50:
        return "Safe", "#2ecc71", "Air quality is good and safe."
    elif aqi <= 100:
        return "Acceptable", "#f1c40f", "Air quality is acceptable."
    elif aqi <= 200:
        return "Moderate", "#f39c12", "Sensitive people should be careful."
    elif aqi <= 300:
        return "Poor", "#e74c3c", "Reduce outdoor activity."
    elif aqi <= 400:
        return "Very Poor", "#8e44ad", "Avoid prolonged outdoor exposure."
    return "Hazardous", "#6e2c00", "Health warning. Stay protected."


def format_time_ampm(value):
    try:
        t = str(value).strip()
        if ":" in t:
            return pd.to_datetime(t, format="%H:%M", errors="coerce").strftime("%I:%M %p")
        return t
    except Exception:
        return str(value)


@app.route("/", methods=["GET"])
def home():
    if not os.path.exists(PROCESSED_FILE):
        return "Processed file not found. Please run process_data.py first."

    df = pd.read_csv(PROCESSED_FILE)

    selected_city = request.args.get("city", "")

    # dropdown list
    cities = sorted(df["location"].dropna().unique().tolist()) if "location" in df.columns else []

    # if nothing selected, don't show city-specific data
    if selected_city == "":
        filtered_df = pd.DataFrame(columns=df.columns)
        avg_aqi = 0
        aqi_status = "Not Selected"
        aqi_color = "#95a5a6"
        aqi_message = "Please select a location to view AQI."
        time_labels = []
        time_values = []
    else:
        filtered_df = df[df["location"] == selected_city].copy()

        avg_aqi = round(filtered_df["aqi"].mean(), 2) if not filtered_df.empty else 0
        aqi_status, aqi_color, aqi_message = get_aqi_status(avg_aqi)

        graph_df = filtered_df.copy()

        if graph_df.empty:
            time_labels = []
            time_values = []
        else:
            if "time_ist" in graph_df.columns:
                graph_df["time_label"] = graph_df["time_ist"].astype(str).str.strip().apply(format_time_ampm)
            elif "date_ist" in graph_df.columns:
                graph_df["time_label"] = graph_df["date_ist"].astype(str).str.strip()
            else:
                graph_df["time_label"] = graph_df.index.astype(str)

            graph_df = graph_df.head(10)
            time_labels = graph_df["time_label"].tolist()
            time_values = graph_df["aqi"].tolist()

    # map data always visible
    map_df = df[["location", "lat", "lon", "aqi"]].dropna().copy()
    map_df = map_df.drop_duplicates(subset=["location"])
    map_data = map_df.to_dict(orient="records")

    return render_template(
        "index.html",
        cities=cities,
        selected_city=selected_city,
        avg_aqi=avg_aqi,
        aqi_status=aqi_status,
        aqi_color=aqi_color,
        aqi_message=aqi_message,
        map_data=map_data,
        time_labels=time_labels,
        time_values=time_values
    )


@app.route("/upload", methods=["POST"])
def upload_file():
    if "datafile" not in request.files:
        return redirect(url_for("home"))

    file = request.files["datafile"]

    if file.filename == "":
        return redirect(url_for("home"))

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        temp_path = filename
        file.save(temp_path)

        if os.path.exists(RAW_FILE):
            os.remove(RAW_FILE)
        os.rename(temp_path, RAW_FILE)

        process_file()

    return redirect(url_for("home"))


if __name__ == "__main__":
    print("Starting Flask server...")
    app.run(debug=True, host="127.0.0.1", port=5000)