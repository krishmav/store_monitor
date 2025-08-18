import uuid
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import FileResponse

app = FastAPI(title="Store Monitor (Demo)")

# Simple in-memory job tracker
REPORTS = {}  # report_id -> {"status": "Running"|"Complete", "file": "path/to/excel"}

@app.post("/trigger_report")
def trigger_report():
    report_id = uuid.uuid4().hex
    REPORTS[report_id] = {"status": "Running", "file": None}

    # Generate the Excel immediately (for demo)
    out_path = f"report_{report_id}.xlsx"
    generate_report(out_path)
    REPORTS[report_id]["status"] = "Complete"
    REPORTS[report_id]["file"] = out_path

    return {"report_id": report_id}

@app.get("/get_report/{report_id}")
def get_report(report_id: str):
    info = REPORTS.get(report_id)
    if not info:
        return {"error": "Invalid report_id"}
    if info["status"] != "Complete":
        return {"status": info["status"]}
    return FileResponse(info["file"], filename="report.xlsx")

def generate_report(file_path: str):
    # Demo data; replace with real calculations later
    data = [
        {"store_id": "A", "uptime_last_hour": 45, "uptime_last_day": 11.25, "uptime_last_week": 56.5,
         "downtime_last_hour": 15, "downtime_last_day": 0.75, "downtime_last_week": 7.5},
        {"store_id": "B", "uptime_last_hour": 30, "uptime_last_day": 9.0, "uptime_last_week": 48.0,
         "downtime_last_hour": 30, "downtime_last_day": 3.0, "downtime_last_week": 12.0},
    ]
    df = pd.DataFrame(data)

    # Save directly as Excel file
    df.to_excel(file_path, index=False, engine="openpyxl")
