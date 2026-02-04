from dagster import asset, AssetExecutionContext
import pandas as pd
from sqlalchemy import create_engine, text
import os
import requests
import json
from dotenv import load_dotenv
from elt_project.core.ml_engine import MLEngine

load_dotenv()

# Database connection string
DB_CONNECTION_STRING = (
    f"mssql+pyodbc://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_SERVER')}/{os.getenv('DB_DATABASE')}?"
    f"driver={os.getenv('DB_DRIVER')}&TrustServerCertificate={os.getenv('DB_TRUST_SERVER_CERTIFICATE')}"
)

@asset(group_name="analytics_ai")
def run_predictive_analytics(context: AssetExecutionContext):
    """
    Reads analytics configuration, fetches data from destination tables,
    runs ML models (Anomaly Detection & Forecasting), and saves results.
    """
    engine = create_engine(DB_CONNECTION_STRING)
    
    # 1. Fetch active analytics configurations
    with engine.connect() as conn:
        configs = pd.read_sql("SELECT * FROM analytics_config WHERE is_active = 1", conn)
    
    if configs.empty:
        context.log.info("No active analytics configurations found.")
        return

    results = []

    for _, row in configs.iterrows():
        target_table = row['target_table']
        date_col = row['date_column']
        value_col = row['value_column']
        model_type = row['model_type']

        context.log.info(f"Running {model_type} on {target_table}...")

        # 2. Fetch source data (Capped at 50k rows for memory safety)
        # We fetch DESC to get the latest data, then sort ASC for the ML model
        query = f"SELECT TOP 50000 {date_col}, {value_col} FROM {target_table} ORDER BY {date_col} DESC"
        try:
            df = pd.read_sql(query, engine)
            df = df.sort_values(by=date_col, ascending=True).reset_index(drop=True)

            # 3. Apply ML Logic (Inside try block to catch ML errors per table)
            if model_type == 'anomaly_detection':
                processed_df = MLEngine.detect_anomalies(df, value_col)
                # Filter only anomalies to save space
                anomalies = processed_df[processed_df['is_anomaly'] == 1].copy()
                anomalies['target_table'] = target_table
                anomalies['model_type'] = 'anomaly_detection'
                anomalies['run_id'] = context.run_id
                anomalies.rename(columns={date_col: 'prediction_date', value_col: 'actual_value'}, inplace=True)
                
                if not anomalies.empty:
                    results.append(anomalies[['run_id', 'target_table', 'model_type', 'prediction_date', 'actual_value', 'is_anomaly', 'anomaly_score']])
                    
                    # --- ALERTING LOGIC ---
                    webhook_url = row.get('alert_webhook_url')
                    if webhook_url:
                        count = len(anomalies)
                        msg = {
                            "text": f"🚨 **Anomaly Alert**: Detected {count} anomalies in `{target_table}`.\n"
                                    f"Check the dashboard for details."
                        }
                        try:
                            requests.post(webhook_url, json=msg)
                            context.log.info(f"Sent alert to webhook for {target_table}")
                        except Exception as req_err:
                            context.log.error(f"Failed to send alert: {req_err}")

            elif model_type == 'forecast':
                forecast_df = MLEngine.generate_forecast(df, date_col, value_col)
                forecast_df['target_table'] = target_table
                forecast_df['run_id'] = context.run_id
                forecast_df.rename(columns={date_col: 'prediction_date'}, inplace=True)
                
                if not forecast_df.empty:
                    results.append(forecast_df[['run_id', 'target_table', 'model_type', 'prediction_date', 'predicted_value']])

        except Exception as e:
            context.log.error(f"Failed to process {target_table}: {e}")
            continue

    # 4. Save results to database
    if results:
        final_df = pd.concat(results)
        # Ensure columns match DB schema
        final_df.to_sql('analytics_predictions', engine, if_exists='append', index=False)
        context.log.info(f"Saved {len(final_df)} prediction records.")
    else:
        context.log.info("No predictions or anomalies generated.")

    return "Analytics Run Complete"