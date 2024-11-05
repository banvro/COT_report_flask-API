import os
import pandas as pd
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta, timezone
import cot_reports
from io import StringIO

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

def read_data_from_txt(report_type):
    file_mapping = {
        'legacy_fut': 'FUT86_16.txt',
        'disaggregated_fut': 'annual.txt',
        'fut_options': 'F_Disagg06_16.txt'
    }
    file_name = file_mapping.get(report_type)

    if file_name and os.path.exists(file_name):
        with open(file_name, 'r') as file:
            file_content = file.read()
            try:
                csv_data = StringIO(file_content)
                # Suppressing DtypeWarning by setting low_memory=False
                df = pd.read_csv(csv_data, low_memory=False)
                
                # Sorting by a hypothetical "timestamp" column, adjust if needed
                df = df.sort_values(by="timestamp", ascending=False).head(5)
                
                # Convert the top 5 records to JSON format
                return df.to_dict(orient='records')
            except Exception as e:
                # If not in CSV format, return as plain text
                return file_content[:500] + '...' if len(file_content) > 500 else file_content
    return None

@app.route('/api/cot_reports', methods=['GET'])
def get_cot_report():
    report_type = request.args.get('report_type')
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 10))

    if not report_type:
        return jsonify({"status": "error", "message": "report_type parameter is required"}), 400

    one_month_ago = datetime.now(timezone.utc) - timedelta(days=30)
    reports_query = CotReport.query.filter_by(report_type=report_type).filter(CotReport.timestamp >= one_month_ago)

    reports = reports_query.order_by(CotReport.timestamp.desc()).paginate(page=page, per_page=page_size, error_out=False).items

    if reports:
        response_data = [{
            "data": report.data[:500] + '...' if len(report.data) > 500 else report.data,
            "timestamp": report.timestamp
        } for report in reports]
        return jsonify({"status": "success", "data": response_data}), 200
    else:
        data = read_data_from_txt(report_type)
        if isinstance(data, list):  # If data is JSON format
            return jsonify({"status": "success", "data": data}), 200
        elif isinstance(data, str):  # If data is plain text
            return jsonify({"status": "success", "data": data}), 200
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
    app.run(debug=True, host='0.0.0.0')
