
import os
import pandas as pd
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta, timezone
import cot_reports
from io import StringIO
import threading  # Import threading module

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///cot_reports.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class CotReport(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    report_type = db.Column(db.String(50), nullable=False)
    data = db.Column(db.Text, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.now(timezone.utc))

def create_db():
    with app.app_context():
        db.create_all()

create_db()

def fetch_latest_cot_data(report_type):
    try:
        data = cot_reports.cot_all_reports(report_type)
        
        if data:
            existing_report = CotReport.query.filter_by(report_type=report_type).order_by(CotReport.timestamp.desc()).first()
            if not existing_report:
                new_report = CotReport(report_type=report_type, data=data)
                db.session.add(new_report)
                db.session.commit()
            else:
                print(f"Latest report for {report_type} already exists in the database.")
                
    except Exception as e:
        print(f"Failed to fetch data for {report_type}: {e}")


def background_fetch_reports():
    report_types = ['legacy_fut', 'disaggregated_fut', 'fut_options']
    for report_type in report_types:
        print(f"Fetching data for {report_type}...")
        fetch_latest_cot_data(report_type)

@app.route("/")
def homepage():
    return "thoomeee"

def read_data_from_txt(report_type):
    # Define the file mapping
    file_mapping = {
        'legacy_fut': 'FinComYY.txt',  # Use FinComYY.txt for legacy_fut
        'disaggregated_fut': 'annual.txt',  # Keeping other mappings unchanged
        'fut_options': 'F_Disagg06_16.txt'
    }
    file_name = file_mapping.get(report_type)

    if file_name and os.path.exists(file_name):
        with open(file_name, 'r') as file:
            file_content = file.read()
            try:
                csv_data = StringIO(file_content)
                df = pd.read_csv(csv_data, low_memory=False)

                # Print the columns for debugging
                print("DataFrame columns:", df.columns.tolist())
                print("First few rows of DataFrame:\n", df.head())

                # Check if 'Market_and_Exchange_Names' column exists and filter data
                if 'Market_and_Exchange_Names' in df.columns:
                    filtered_df = df[df['Market_and_Exchange_Names'] == 'USD INDEX - ICE FUTURES U.S.']
                    if not filtered_df.empty:
                        # Sort data by 'As of Date in Form YYYY-MM-DD' if that column exists
                        if 'As of Date in Form YYYY-MM-DD' in filtered_df.columns:
                            filtered_df = filtered_df.sort_values(by="As of Date in Form YYYY-MM-DD", ascending=False).head(5)
                        return filtered_df.to_dict(orient='records')
                    else:
                        print("No records found for 'USD INDEX - ICE FUTURES U.S.'")
                        return None
                else:
                    print("'Market_and_Exchange_Names' column not found.")
                    return None
            except Exception as e:
                print(f"Error reading CSV data: {e}")
                return None
    return None



@app.route('/api/cot_reports', methods=['GET'])
def get_cot_report():
    report_type = request.args.get('report_type')
    commodity_code = request.args.get('commodity_code')  # New parameter for filtering
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 10))

    if not report_type:
        return jsonify({"status": "error", "message": "report_type parameter is required"}), 400

    # Check if the request is specifically for the "DXY USD Index"
    if report_type.lower() == "dxy usd index":
        latest_report = CotReport.query.filter_by(report_type=report_type) \
            .order_by(CotReport.timestamp.desc()) \
            .first()  # Fetch only the most recent report
        
        if latest_report:
            response_data = {
                "data": latest_report.data[:500] + '...' if len(latest_report.data) > 500 else latest_report.data,
                "timestamp": latest_report.timestamp
            }
            return jsonify({"status": "success", "data": response_data}), 200
        else:
            return jsonify({"status": "error", "message": "No report found for the DXY USD Index"}), 404

    # Build the base query for the cot reports
    reports_query = CotReport.query.filter_by(report_type=report_type)

    # Apply additional filter if commodity_code is provided
    if commodity_code:
        # Assuming 'commodity_code' is a field in your CotReport model; adjust as necessary.
        reports_query = reports_query.filter(CotReport.data.like(f'%{commodity_code}%'))  # Example condition

    # Optionally filter by timestamp (last 30 days)
    one_month_ago = datetime.now(timezone.utc) - timedelta(days=30)
    reports_query = reports_query.filter(CotReport.timestamp >= one_month_ago)

    # Paginate the query results
    reports = reports_query.order_by(CotReport.timestamp.desc()).paginate(page=page, per_page=page_size, error_out=False).items

    if reports:
        response_data = [{
            "data": report.data[:500] + '...' if len(report.data) > 500 else report.data,
            "timestamp": report.timestamp
        } for report in reports]
        return jsonify({"status": "success", "data": response_data}), 200
    else:
        data = read_data_from_txt(report_type)
        if isinstance(data, list):  # If data is in JSON format
            return jsonify({"status": "success", "data": data[0]}), 200
        else:
            return jsonify({"status": "error", "message": "No report found for the given report_type"}), 404


@app.route('/api/fetch_cot_data', methods=['POST'])
def fetch_cot_data_endpoint():
    report_type = request.json.get('report_type')

    if not report_type:
        return jsonify({"status": "error", "message": "report_type parameter is required"}), 400

    try:
        fetch_latest_cot_data(report_type)
        return jsonify({"status": "success", "message": f"Data for {report_type} fetched successfully"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


def background_fetch_reports():
    report_types = ['legacy_fut', 'disaggregated_fut', 'fut_options']
    for report_type in report_types:
        print(f"Fetching data for {report_type}...")
        fetch_latest_cot_data(report_type)
    # Schedule the next fetch in 60 seconds
    threading.Timer(60, background_fetch_reports).start()

if __name__ == '__main__':
    # thread = threading.Thread(target=background_fetch_reports)
    # thread.daemon = True  # This makes sure the thread will exit when the main program does
    # thread.start()
    threading.Thread(target=background_fetch_reports, daemon=True).start()
    
    print("Background thread started.")
        
    app.run(debug=True, host='0.0.0.0')
