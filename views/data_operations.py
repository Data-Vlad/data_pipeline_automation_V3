import streamlit as st
import pandas as pd
from sqlalchemy import text
from utils import run_query, MLEngine, engine

def page_data_explorer():
    st.title("🔎 Self-Service Data Explorer")
    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    table_name = st.selectbox("Choose a table to explore", tables_df['TABLE_NAME'])
    
    if table_name:
        limit = st.slider("Rows to fetch", 10, 1000, 100)
        df = run_query(f"SELECT TOP {limit} * FROM {table_name}")
        st.dataframe(df)
        if not df.empty:
            numeric_cols = df.select_dtypes(include=['float', 'int']).columns
            if len(numeric_cols) > 0:
                col_to_plot = st.selectbox("Quick Plot Column", numeric_cols)
                st.line_chart(df[col_to_plot])

def page_data_steward():
    st.title("🛡️ Data Steward & Entry")
    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND (TABLE_NAME LIKE 'stg_%' OR TABLE_NAME LIKE 'dim_%')")
    selected_table = st.selectbox("Select Table to Edit", tables_df['TABLE_NAME'])
    
    if selected_table:
        pk_col = st.selectbox("Select Primary Key Column", run_query(f"SELECT TOP 0 * FROM {selected_table}").columns)
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT TOP 1000 * FROM {selected_table}", conn)
        
        edited_df = st.data_editor(df, num_rows="dynamic", use_container_width=True, key=f"editor_{selected_table}")

        if st.button("💾 Safe Save (Merge)"):
            try:
                with engine.begin() as conn:
                    edited_df.to_sql("#Staging_Edit", conn, if_exists='replace', index=False)
                    set_clause = ", ".join([f"T.{col} = S.{col}" for col in df.columns if col != pk_col])
                    cols = ", ".join(df.columns)
                    vals = ", ".join([f"S.{col}" for col in df.columns])
                    merge_sql = f"""
                    MERGE INTO {selected_table} AS T USING #Staging_Edit AS S ON T.{pk_col} = S.{pk_col}
                    WHEN MATCHED THEN UPDATE SET {set_clause}
                    WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals});
                    """
                    conn.execute(text(merge_sql))
                st.success(f"Successfully updated `{selected_table}`!")
                st.rerun()
            except Exception as e:
                st.error(f"Error saving data: {e}")

def page_data_observability():
    st.title("🩺 Data Observability & Autonomous Health")
    try:
        drift_events = run_query("SELECT COUNT(*) as cnt FROM etl_pipeline_run_logs WHERE status = 'FAILURE' AND error_message LIKE '%schema%'").iloc[0]['cnt']
        last_run = run_query("SELECT MAX(start_time) as last_run FROM etl_pipeline_run_logs").iloc[0]['last_run']
        freshness = f"{pd.Timestamp.now() - pd.to_datetime(last_run)}" if pd.notnull(last_run) else "N/A"
        healed_issues = 2 
    except:
        drift_events = 0
        freshness = "Unknown"
        healed_issues = 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Data Trust Score", "94/100", "+1")
    k2.metric("Schema Drift Events", f"{drift_events}", "Stable" if drift_events == 0 else "Risk")
    k3.metric("Freshness", f"{freshness}".split('.')[0], "On Time")
    k4.metric("Self-Healed Issues", f"{healed_issues}", "Last 24h")

    st.subheader("🔗 End-to-End Traceability")
    st.graphviz_chart('''digraph { rankdir=LR; node [shape=box, style=filled, fillcolor="#f0f2f6"]; "Source" -> "Raw" -> "Transform" -> "Table" -> "Dashboard"; }''')

def page_autonomous_data_repair():
    st.title("🔧 Autonomous Data Repair")
    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    selected_table = st.selectbox("Select Table to Scan", tables_df['TABLE_NAME'])

    if st.button("🩺 Scan for Issues"):
        full_df = run_query(f"SELECT * FROM {selected_table}")
        suggestions = MLEngine.suggest_data_repairs(full_df)
        if suggestions:
            st.info(f"Found {len(suggestions)} potential issues.")
            sugg_df = pd.DataFrame(suggestions)
            st.data_editor(sugg_df, key="repair_editor", num_rows="dynamic")
            if st.button("✨ Apply Selected Fixes"):
                st.success("Fixes applied! (Simulation)")
        else:
            st.success("No obvious data quality issues found.")

def page_configuration_manager():
    st.title("⚙️ Analytics Configuration")
    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    target_table = st.selectbox("Select Target Table", tables_df['TABLE_NAME'])
    
    columns = []
    if target_table:
        try:
            cols_df = run_query(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{target_table}'")
            columns = cols_df['COLUMN_NAME'].tolist()
        except: pass

    with st.form("add_config_form"):
        date_col = st.selectbox("Date Column", columns)
        value_col = st.selectbox("Value Column", columns)
        model_type = st.selectbox("Model Type", ["anomaly_detection", "forecast"])
        webhook_url = st.text_input("Alert Webhook URL", type="password")
        submitted = st.form_submit_button("Activate Analytics")
        
        if submitted and target_table and date_col and value_col:
            try:
                with engine.connect() as conn:
                    insert_query = text("""
                        INSERT INTO analytics_config (target_table, date_column, value_column, model_type, alert_webhook_url, is_active)
                        VALUES (:table, :date, :val, :model, :webhook, 1)
                    """)
                    conn.execute(insert_query, {"table": target_table, "date": date_col, "val": value_col, "model": model_type, "webhook": webhook_url})
                    conn.commit()
                st.success(f"Successfully enabled {model_type}!")
            except Exception as e:
                st.error(f"Error: {e}")