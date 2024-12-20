import os
import pandas as pd
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta, timezone
import cot_reports
from io import StringIO
import threading
import logging

# Configure logging
# logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///cot_reports.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class CotReport(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    report_type = db.Column(db.String(50), nullable=False)
    data = db.Column(db.Text, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.now(timezone.utc))

# Create database tables
with app.app_context():
    db.create_all()

import cot_reports 

def fetch_latest_cot_data(report_type):
    try:
        # Fetch new data
        data = cot_reports.cot_all_reports(report_type)
        
        if data:
            # Define file path based on report type
            file_mapping = {
                'legacy_fut': 'FinComYY.txt',
                'gold': 'annual.txt',
                'fut_options': 'F_Disagg06_16.txt'
            }
            file_name = file_mapping.get(report_type)
            
            if file_name:
                # Write new data to the file, overwriting any existing content
                with open(file_name, 'w') as file:
                    file.write(data)
                # logging.info(f"File {file_name} updated with new data for {report_type}.")
                
    except Exception as e:
        logging.error(f"Failed to fetch or write data for {report_type}: {e}")


def background_fetch_reports():
    # print("stearttttttttttttttt")
    with app.app_context():  # Ensure the application context is active
        report_types = ['legacy_fut', 'gold', 'fut_options']
        for report_type in report_types:
            # logging.info(f"Fetching data for {report_type}...")
            fetch_latest_cot_data(report_type)
        
        # Schedule the next fetch in 60 seconds
        # logging.info("Background fetch complete. Scheduling next fetch in 60 seconds.")
        threading.Timer(60, background_fetch_reports).start()

@app.route("/")
def homepage():
    return "Welcome to the COT Reports API!"


def read_data_from_txt(report_type):
    # print(report_type, "eeeeeeeeeeeeeeee")
    file_mapping = {
        'legacy_fut': 'FinComYY.txt',
        'gold': 'annual.txt',
        'british_pound': 'annual.txt',
        'euro_currency': 'annual.txt',
        'jpy_currency': 'annual.txt',
        'fut_options': 'F_Disagg06_16.txt'
    }
    file_name = file_mapping.get(report_type)
    # print(file_name, "ooooooooooooooooooo")
    if file_name and os.path.exists(file_name):
        with open(file_name, 'r') as file:
            file_content = file.read() 
            try:
                csv_data = StringIO(file_content)
                df = pd.read_csv(csv_data, low_memory=False)
                # logging.info("DataFrame columns: %s", df.columns.tolist())
                
                if 'Market_and_Exchange_Names' in df.columns:
                    if report_type == "legacy_fut":
                        filtered_df = df[df['Market_and_Exchange_Names'] == 'USD INDEX - ICE FUTURES U.S.']
                        if not filtered_df.empty:
                            if 'As of Date in Form YYYY-MM-DD' in filtered_df.columns:
                                filtered_df = filtered_df.sort_values(by="As of Date in Form YYYY-MM-DD", ascending=False).head(5)
                            return filtered_df.to_dict(orient='records')
                        else:
                            logging.info("No records found for 'USD INDEX - ICE FUTURES U.S.'")
                            return None
                elif report_type == "gold":
                    filtered_df = df[df['Market and Exchange Names'] == 'GOLD - COMMODITY EXCHANGE INC.']
                    if not filtered_df.empty:
                        if 'As of Date in Form YYYY-MM-DD' in filtered_df.columns:
                            filtered_df = filtered_df.sort_values(by="As of Date in Form YYYY-MM-DD", ascending=False).head(5)
                        return filtered_df.to_dict(orient='records')
                    else:
                        logging.info("No records found for 'GOLD - COMMODITY EXCHANGE INC.'")
                        return None
                elif report_type == "british_pound":
                    filtered_df = df[df['Market and Exchange Names'] == 'BRITISH POUND - CHICAGO MERCANTILE EXCHANGE']
                    if not filtered_df.empty:
                        if 'As of Date in Form YYYY-MM-DD' in filtered_df.columns:
                            filtered_df = filtered_df.sort_values(by="As of Date in Form YYYY-MM-DD", ascending=False).head(5)
                        return filtered_df.to_dict(orient='records')
                    else:
                        logging.info("No records found for 'BRITISH POUND - CHICAGO MERCANTILE EXCHANGE'")
                        return None
                elif report_type == "euro_currency":
                    filtered_df = df[df['Market and Exchange Names'] == 'EURO FX - CHICAGO MERCANTILE EXCHANGE']
                    if not filtered_df.empty:
                        if 'As of Date in Form YYYY-MM-DD' in filtered_df.columns:
                            filtered_df = filtered_df.sort_values(by="As of Date in Form YYYY-MM-DD", ascending=False).head(5)
                        return filtered_df.to_dict(orient='records')
                    else:
                        logging.info("No records found for 'EURO FX - CHICAGO MERCANTILE EXCHANGE'")
                        return None
                elif report_type == "jpy_currency":
                    filtered_df = df[df['Market and Exchange Names'] == 'JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE']
                    if not filtered_df.empty:
                        if 'As of Date in Form YYYY-MM-DD' in filtered_df.columns:
                            filtered_df = filtered_df.sort_values(by="As of Date in Form YYYY-MM-DD", ascending=False).head(5)
                        return filtered_df.to_dict(orient='records')
                    else:
                        logging.info("No records found for 'EURO FX - CHICAGO MERCANTILE EXCHANGE'")
                        return None
                else:
                    logging.warning("'Market_and_Exchange_Names' column not found.")
                    return None
            except Exception as e:
                logging.error(f"Error reading CSV data: {e}")
                return None
    return None



read_data_from_txt("gold")






@app.route('/api/cot_reports', methods=['GET'])
def get_cot_report():
    report_type = request.args.get('report_type')
    commodity_code = request.args.get('commodity_code')
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 10))

    if not report_type:
        return jsonify({"status": "error", "message": "report_type parameter is required"}), 400

    if report_type.lower() == "dxy usd index":
        latest_report = CotReport.query.filter_by(report_type=report_type).order_by(CotReport.timestamp.desc()).first()
        
        if latest_report:
            response_data = {
                "data": latest_report.data[:500] + '...' if len(latest_report.data) > 500 else latest_report.data,
                "timestamp": latest_report.timestamp
            }
            return jsonify({"status": "success", "data": response_data}), 200
        else:
            return jsonify({"status": "error", "message": "No report found for the DXY USD Index"}), 404

    reports_query = CotReport.query.filter_by(report_type=report_type)
    if commodity_code:
        reports_query = reports_query.filter(CotReport.data.like(f'%{commodity_code}%'))

    one_month_ago = datetime.now(timezone.utc) - timedelta(days=30)
    reports_query = reports_query.filter(CotReport.timestamp >= one_month_ago)

    reports = reports_query.order_by(CotReport.timestamp.desc()).paginate(page=page, per_page=page_size, error_out=False).items

    if reports:
        response_data = [{"data": report.data[:500] + '...' if len(report.data) > 500 else report.data, "timestamp": report.timestamp} for report in reports]
        return jsonify({"status": "success", "data": response_data}), 200
    else:
        print("am hereeeeeeeeeeeeee")
        data = read_data_from_txt(report_type)
        if isinstance(data, list):
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

if __name__ == '__main__':
    threading.Thread(target=background_fetch_reports, daemon=True).start()
    logging.info("Background thread started.")
    app.run(debug=True, host='0.0.0.0')
