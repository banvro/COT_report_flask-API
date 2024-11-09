import os
import sys
from main import fetch_latest_cot_data  # Import the function from your Flask app

def background_fetch_reports():
    print("Fetching COT reports...")
    report_types = ['legacy_fut', 'disaggregated_fut', 'fut_options']
    for report_type in report_types:
        print(f"Fetching data for {report_type}...")
        fetch_latest_cot_data(report_type)

if __name__ == "__main__":
    background_fetch_reports()
