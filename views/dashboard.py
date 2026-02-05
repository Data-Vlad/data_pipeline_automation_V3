import streamlit as st
from utils import run_query, MLEngine

def page_dashboard():
    st.title("🚀 Executive Dashboard")
    st.markdown("Real-time overview of pipeline health and key metrics.")
    
    col1, col2, col3 = st.columns(3)
    
    # Use cached query function
    run_count = run_query("SELECT COUNT(*) as cnt FROM etl_pipeline_run_logs WHERE status = 'SUCCESS'").iloc[0]['cnt']
    fail_count = run_query("SELECT COUNT(*) as cnt FROM etl_pipeline_run_logs WHERE status = 'FAILURE'").iloc[0]['cnt']
    anomaly_count = run_query("SELECT COUNT(*) as cnt FROM analytics_predictions WHERE is_anomaly = 1").iloc[0]['cnt']

    col1.metric("Total Successful Runs", run_count)
    col2.metric("Failed Runs", fail_count, delta_color="inverse")
    col3.metric("AI Detected Anomalies", anomaly_count, delta_color="inverse")

    # Automated Data Storytelling
    st.subheader("📝 Automated Data Story")
    success_rate = (run_count / (run_count + fail_count)) * 100 if (run_count + fail_count) > 0 else 0
    
    metrics = {"success_rate": success_rate, "anomalies": anomaly_count, "total_runs": run_count + fail_count, "failed": fail_count}
    story = MLEngine.generate_data_story(metrics, context_str="Data Pipeline Automation System V3")
    st.info(story)

    st.subheader("Recent Activity")
    history = run_query("SELECT TOP 10 pipeline_name, start_time, status FROM etl_pipeline_run_logs ORDER BY start_time DESC")
    st.dataframe(history, use_container_width=True)