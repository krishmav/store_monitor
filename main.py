# main.py
import os
import uuid
import pandas as pd
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from datetime import datetime, timedelta
import pytz

app = FastAPI()

# directory to store reports
REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

# in-memory report registry
reports = {}  # {report_id: {"status": "Running/Complete/Failed", "path": str | None}}

# ---- Utility functions ----
def get_timezone(store_timezone: str):
    try:
        return pytz.timezone(store_timezone)
    except Exception:
        return pytz.timezone("America/Chicago")

def load_data():
    # you can adjust file paths as per your data folder
    stores = pd.read_csv("data/stores.csv")
    business_hours = pd.read_csv("data/business_hours.csv")
    status = pd.read_csv("data/store_status.csv", parse_dates=["timestamp_utc"])
    return stores, business_hours, status

def is_open_at(store_id, timestamp, business_hours, tz):
    """Check if store should be open at given local timestamp"""
    bh = business_hours[business_hours["store_id"] == store_id]
    if bh.empty:
        return True  # default 24/7

    local_time = timestamp.astimezone(tz)
    weekday = local_time.weekday()  # Monday=0
    day_hours = bh[bh["day"] == weekday]

    if day_hours.empty:
        return False

    for _, row in day_hours.iterrows():
        start = datetime.strptime(row["start_time_local"], "%H:%M:%S").time()
        end = datetime.strptime(row["end_time_local"], "%H:%M:%S").time()
        if start <= local_time.time() <= end:
            return True
    return False

def compute_metrics(store_id, status_df, business_hours, tz):
    """Compute uptime/downtime metrics for last hour, day, week"""
    now = datetime.now(tz)
    windows = {
        "last_hour": now - timedelta(hours=1),
        "last_day": now - timedelta(days=1),
        "last_week": now - timedelta(days=7),
    }

    results = {
        "uptime_last_hour(in minutes)": 0,
        "uptime_last_day(in hours)": 0,
        "uptime_last_week(in hours)": 0,
        "downtime_last_hour(in minutes)": 0,
        "downtime_last_day(in hours)": 0,
        "downtime_last_week(in hours)": 0,
    }

    store_status = status_df[status_df["store_id"] == store_id]

    for label, start_time in windows.items():
        window_df = store_status[store_status["timestamp_utc"] >= start_time]

        uptime = downtime = 0
        for _, row in window_df.iterrows():
            ts = row["timestamp_utc"].tz_localize("UTC")
            ts = ts.astimezone(tz)

            if not is_open_at(store_id, ts, business_hours, tz):
                continue

            if row["status"] == "active":
                uptime += 1
            else:
                downtime += 1

        if "hour" in label:
            results[f"uptime_{label}(in minutes)"] = uptime
            results[f"downtime_{label}(in minutes)"] = downtime
        else:
            # scale minutes to hours
            results[f"uptime_{label}(in hours)"] = round(uptime / 60, 2)
            results[f"downtime_{label}(in hours)"] = round(downtime / 60, 2)

    return results

def generate_report(report_id: str):
    try:
        stores, business_hours, status = load_data()

        rows = []
        for _, store in stores.iterrows():
            tz = get_timezone(store.get("timezone", "America/Chicago"))
            metrics = compute_metrics(store["store_id"], status, business_hours, tz)
            row = {"store_id": store["store_id"]}
            row.update(metrics)
            rows.append(row)

        df = pd.DataFrame(rows)
        report_path = os.path.join(REPORT_DIR, f"{report_id}.csv")
        df.to_csv(report_path, index=False)

        reports[report_id]["status"] = "Complete"
        reports[report_id]["path"] = report_path
    except Exception as e:
        reports[report_id]["status"] = "Failed"
        print("Report generation error:", e)

# ---- API endpoints ----
@app.post("/trigger_report")
def trigger_report(background_tasks: BackgroundTasks):
    report_id = str(uuid.uuid4())
    reports[report_id] = {"status": "Running", "path": None}
    background_tasks.add_task(generate_report, report_id)
    return {"report_id": report_id}

@app.get("/get_report")
def get_report(report_id: str):
    if report_id not in reports:
        return JSONResponse({"error": "Report not found"}, status_code=404)

    report = reports[report_id]
    if report["status"] == "Complete":
        return FileResponse(report["path"], filename=f"{report_id}.csv")
    return {"status": report["status"]}
