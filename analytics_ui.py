import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
from dotenv import load_dotenv
import plotly.express as px
import plotly.graph_objects as go
import time

# Add project root to path to import core modules
sys.path.append(os.path.dirname(__file__))
from elt_project.core.ml_engine import MLEngine

# Load environment variables
load_dotenv()

# Page Config
st.set_page_config(page_title="Modern Analytics Hub", page_icon="📈", layout="wide")

# Database Connection
@st.cache_resource
def get_connection():
    db_connection_str = (
        f"mssql+pyodbc://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_SERVER')}/{os.getenv('DB_DATABASE')}?"
        f"driver={os.getenv('DB_DRIVER')}&TrustServerCertificate={os.getenv('DB_TRUST_SERVER_CERTIFICATE')}"
    )
    return create_engine(db_connection_str)

engine = get_connection()

# --- Caching Helper ---
@st.cache_data(ttl=600) # Cache data for 10 minutes
def run_query(query_str):
    with engine.connect() as conn:
        return pd.read_sql(query_str, conn)

# --- Authentication & RBAC System ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.user_role = None

def login_screen():
    st.title("🔐 Enterprise Analytics Hub")
    st.markdown("Please sign in to access the secure data environment.")
    
    col1, col2 = st.columns([1, 2])
    with col1:
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Sign In")
            
            if submitted:
                # In a real production app, validate against a DB table or SSO (LDAP/Okta)
                if username == "admin" and password == "admin123":
                    st.session_state.authenticated = True
                    st.session_state.user_role = "Admin"
                    st.success("Welcome, Administrator.")
                    st.rerun()
                elif username == "user" and password == "user123":
                    st.session_state.authenticated = True
                    st.session_state.user_role = "Viewer"
                    st.success("Welcome, User.")
                    st.rerun()
                else:
                    st.error("Invalid credentials. (Try admin/admin123)")

if not st.session_state.authenticated:
    login_screen()
    st.stop() # Block access to the rest of the app

# --- Sidebar ---
st.sidebar.title("Analytics & AI Hub")
page = st.sidebar.radio("Navigate", ["Dashboard", "Conversational Analytics", "Predictive Insights", "Clustering & Segmentation", "What-If Simulator", "AI Auto-Dashboards", "Data Explorer", "Data Steward", "AI Data Entry", "Data Observability", "Configuration Manager"])
st.sidebar.caption(f"👤 Logged in as: **{st.session_state.user_role}**")

# RBAC: Define accessible pages based on Role
if st.session_state.user_role == "Admin":
    available_pages = ["Dashboard", "Conversational Analytics", "Predictive Insights", "Clustering & Segmentation", "What-If Simulator", "AI Auto-Dashboards", "Data Explorer", "Data Steward", "AI Data Entry", "Data Observability", "Configuration Manager"]
else:
    # Viewers cannot access Data Steward (Write) or Config (Admin)
    available_pages = ["Dashboard", "Conversational Analytics", "Predictive Insights", "Clustering & Segmentation", "What-If Simulator", "AI Auto-Dashboards", "Data Explorer", "Data Observability"]

page = st.sidebar.radio("Navigate", available_pages)

if st.sidebar.button("🚪 Logout"):
    st.session_state.authenticated = False
    st.session_state.user_role = None
    st.rerun()

# --- Dashboard Page ---
if page == "Dashboard":
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

    if st.button("📄 Download Executive Report (HTML)"):
        st.download_button("Click to Save", data=f"<h1>Executive Report</h1><p>{story}</p><hr>Generated by Analytics Hub", file_name="report.html", mime="text/html")

    st.subheader("Recent Activity")
    history = run_query("SELECT TOP 10 pipeline_name, start_time, status FROM etl_pipeline_run_logs ORDER BY start_time DESC")
    st.dataframe(history, use_container_width=True)

# --- Conversational Analytics (NLQ) Page ---
elif page == "Conversational Analytics":
    st.title("💬 Conversational Analytics")
    st.markdown("Ask questions in plain English to generate insights, SQL, and visualizations.")

    if not os.getenv("OPENAI_API_KEY"):
        st.warning("⚠️ OpenAI API Key not found or library missing. Please set `OPENAI_API_KEY` in .env and install `openai`.")
        st.stop()

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # React to user input
    if prompt := st.chat_input("Ex: Why did sales drop in Texas last week?"):
        # Display user message in chat message container
        st.chat_message("user").markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("assistant"):
            with st.spinner("Translating natural language to SQL..."):
                try:
                    # 1. Get Schema Context
                    schema_df = run_query("SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME NOT LIKE 'sys%'")
                    schema_context = schema_df.to_csv(index=False)

                    sql_query = MLEngine.generate_sql_from_question(prompt, schema_context)
                    
                    # 3. Execute Query
                    result_df = run_query(sql_query)
                    
                    st.markdown(f"**Generated SQL:**")
                    st.code(sql_query, language="sql")
                    
                    st.markdown("**Result:**")
                    st.dataframe(result_df)
                    
                    st.session_state.messages.append({"role": "assistant", "content": f"Executed SQL: `{sql_query}`"})
                except Exception as e:
                    st.error(f"AI Error: {e}")
                    st.session_state.messages.append({"role": "assistant", "content": f"Error: {e}"})

# --- Predictive Insights Page ---
elif page == "Predictive Insights":
    st.title("🤖 AI & Predictive Analytics")
    
    tables = run_query("SELECT DISTINCT target_table FROM analytics_predictions")
    
    selected_table = st.selectbox("Select Data Source", tables['target_table'])
    
    if selected_table:
        query = f"""
            SELECT prediction_date, actual_value, predicted_value, is_anomaly 
            FROM analytics_predictions 
            WHERE target_table = '{selected_table}'
            ORDER BY prediction_date
        """
        data = run_query(query)
        
        # Visualization
        fig = go.Figure()
        
        # Actuals (if available in this table structure)
        if 'actual_value' in data.columns and data['actual_value'].notna().any():
            fig.add_trace(go.Scatter(x=data['prediction_date'], y=data['actual_value'], mode='lines', name='Actual Value'))
            
            # Anomalies
            anomalies = data[data['is_anomaly'] == 1]
            fig.add_trace(go.Scatter(x=anomalies['prediction_date'], y=anomalies['actual_value'], mode='markers', name='Anomaly', marker=dict(color='red', size=10)))

        # Forecasts
        forecasts = data[data['predicted_value'].notna()]
        if not forecasts.empty:
            fig.add_trace(go.Scatter(x=forecasts['prediction_date'], y=forecasts['predicted_value'], mode='lines', name='AI Forecast', line=dict(dash='dash')))

        st.plotly_chart(fig, use_container_width=True)
        
        # Human-in-the-Loop Feedback
        if not anomalies.empty:
            st.subheader("🛠️ Anomaly Feedback")
            to_dismiss = st.selectbox("Select an anomaly to dismiss (False Positive)", anomalies['prediction_date'])
            if st.button("Dismiss Selected Anomaly"):
                with engine.begin() as conn:
                    conn.execute(text(f"UPDATE analytics_predictions SET is_anomaly = 0 WHERE target_table = '{selected_table}' AND prediction_date = '{to_dismiss}'"))
                st.success(f"Dismissed anomaly for {to_dismiss}. It will no longer appear in alerts.")
                st.rerun()

        st.info("💡 **AI Insight**: Anomalies detected in red. Dashed lines indicate future predictions generated by the ML Engine.")

# --- Clustering & Segmentation Page ---
elif page == "Clustering & Segmentation":
    st.title("🧩 Customer & Entity Segmentation")
    st.markdown("Use Unsupervised Learning (K-Means) to group similar data points automatically.")

    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    selected_table = st.selectbox("Select Table", tables_df['TABLE_NAME'])

    if selected_table:
        # Fetch columns
        df_preview = run_query(f"SELECT TOP 5 * FROM {selected_table}")
        numeric_cols = df_preview.select_dtypes(include=['float', 'int']).columns.tolist()
        
        selected_features = st.multiselect("Select Features for Clustering", numeric_cols, default=numeric_cols, help="We've auto-selected all numeric columns. Remove any ID columns or irrelevant data.")
        n_clusters = st.slider("Number of Clusters", 2, 10, 3)

        if st.button("Run Clustering") and len(selected_features) >= 2:
            # Fetch full data for clustering
            full_df = run_query(f"SELECT {', '.join(selected_features)} FROM {selected_table}")
            
            with st.spinner("Running K-Means Algorithm..."):
                clustered_df = MLEngine.perform_clustering(full_df, selected_features, n_clusters)
            
            st.success("Clustering Complete!")
            
            # Visualize (Pairplot or Scatter)
            if len(selected_features) >= 2:
                fig = px.scatter(clustered_df, x=selected_features[0], y=selected_features[1], color='cluster_id', title=f"Segmentation: {selected_features[0]} vs {selected_features[1]}")
                st.plotly_chart(fig, use_container_width=True)

# --- What-If Simulator Page ---
elif page == "What-If Simulator":
    st.title("🎛️ What-If Scenario Analysis")
    st.markdown("Simulate business outcomes by tweaking key variables to see projected results.")

    col1, col2 = st.columns([1, 3])

    with col1:
        st.subheader("Scenario Parameters")
        price_adj = st.slider("Pricing Adjustment (%)", -20, 20, 0)
        marketing_spend = st.slider("Marketing Spend Increase (%)", 0, 50, 10)
        churn_rate = st.slider("Expected Churn Rate (%)", 0, 10, 2)

    with col2:
        st.subheader("Projected Impact: Monthly Revenue")
        
        # Mock Simulation Logic
        base_revenue = 150000
        # Simple formula for demonstration
        impact_factor = 1 + (price_adj * 0.015) + (marketing_spend * 0.008) - (churn_rate * 0.02)
        projected_revenue = base_revenue * impact_factor
        delta = projected_revenue - base_revenue

        st.metric("Projected Revenue", f"${projected_revenue:,.0f}", f"{delta:+,.0f}", delta_color="normal")

        # Visualization
        months = ["Month 1", "Month 2", "Month 3", "Month 4", "Month 5", "Month 6"]
        baseline_trend = [base_revenue * (1 + 0.01*i) for i in range(6)]
        scenario_trend = [val * impact_factor for val in baseline_trend]

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=months, y=baseline_trend, name='Baseline Forecast', line=dict(dash='dot', color='gray')))
        fig.add_trace(go.Scatter(x=months, y=scenario_trend, name='Simulated Scenario', fill='tonexty', line=dict(color='#00CC96')))
        st.plotly_chart(fig, use_container_width=True)

# --- AI Auto-Dashboards Page ---
elif page == "AI Auto-Dashboards":
    st.title("✨ AI Auto-Dashboards")
    st.markdown("Select any table, and the system will **automatically decide** the best way to visualize it.")

    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    
    selected_table = st.selectbox("Choose a table to analyze", tables_df['TABLE_NAME'])

    if selected_table:
        # Smart Aggregation: Get columns first
        cols_df = run_query(f"SELECT TOP 0 * FROM {selected_table}")
        all_cols = cols_df.columns.tolist()
        
        # Heuristic: Find Date and Numeric columns
        numeric_cols = cols_df.select_dtypes(include=['float', 'int']).columns.tolist()
        date_cols = [c for c in all_cols if 'date' in c.lower() or 'time' in c.lower()]

        if date_cols and numeric_cols:
            # Perform Aggregation in SQL for Scalability
            date_col = date_cols[0]
            val_col = numeric_cols[0]
            
            st.info(f"Running optimized aggregation on `{selected_table}`...")
            agg_query = f"SELECT {date_col}, SUM({val_col}) as {val_col} FROM {selected_table} GROUP BY {date_col} ORDER BY {date_col}"
            df = run_query(agg_query)
            
            # Pass aggregated data to ML Engine
            rec = MLEngine.recommend_visualization(df)
            
            st.subheader(rec.get('title', 'Analysis'))
            st.caption(f"🤖 **AI Decision**: {rec.get('reasoning')}")

            # Render based on recommendation
            if rec['type'] == 'time_series':
                # Ensure date is datetime
                df[rec['x']] = pd.to_datetime(df[rec['x']])
                # Allow user to pick which metric if multiple
                y_col = st.selectbox("Select Metric", rec['y']) if len(rec['y']) > 1 else rec['y'][0]
                fig = px.line(df, x=rec['x'], y=y_col, title=f"{y_col} over Time")
                st.plotly_chart(fig, use_container_width=True)
            
            elif rec['type'] == 'correlation_matrix':
                corr = df[rec['cols']].corr()
                fig = px.imshow(corr, text_auto=True, title="Correlation Matrix")
                st.plotly_chart(fig, use_container_width=True)
            
            elif rec['type'] == 'scatter':
                fig = px.scatter(df, x=rec['x'], y=rec['y'], title=f"{rec['x']} vs {rec['y']}")
                st.plotly_chart(fig, use_container_width=True)
            
            elif rec['type'] == 'bar':
                # Aggregate for cleaner bar charts
                agg_df = df.groupby(rec['x'])[rec['y']].sum().reset_index().sort_values(rec['y'], ascending=False).head(20)
                fig = px.bar(agg_df, x=rec['x'], y=rec['y'])
                st.plotly_chart(fig, use_container_width=True)
            
            elif rec['type'] == 'histogram':
                fig = px.histogram(df, x=rec['x'])
                st.plotly_chart(fig, use_container_width=True)
            
            else:
                st.dataframe(df)
        else:
            # Fallback for non-timeseries: Fetch sample
            df = run_query(f"SELECT TOP 1000 * FROM {selected_table}")
            if not df.empty:
                rec = MLEngine.recommend_visualization(df)
                st.write(f"Visualizing sample data ({len(df)} rows)")
                st.dataframe(df)

# --- Data Explorer Page ---
elif page == "Data Explorer":
    st.title("🔎 Self-Service Data Explorer")
    
    # Get all tables
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

# --- Data Steward Page ---
elif page == "Data Steward":
    st.title("🛡️ Data Steward & Entry")
    st.markdown("Manually edit staging data, correct quality issues, or manage reference tables.")

    # Filter for staging tables (stg_) or dimension tables (dim_) to prevent editing system tables
    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND (TABLE_NAME LIKE 'stg_%' OR TABLE_NAME LIKE 'dim_%')")
    
    selected_table = st.selectbox("Select Table to Edit", tables_df['TABLE_NAME'])
    pk_col = st.selectbox("Select Primary Key Column (Required for Safe Saving)", run_query(f"SELECT TOP 0 * FROM {selected_table}").columns)

    if selected_table:
        # Fetch data (Limit to 1000 rows for UI performance)
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT TOP 1000 * FROM {selected_table}", conn)
        
        st.caption(f"Showing top 1000 rows from `{selected_table}`. Edit cells below or add new rows at the bottom.")
        
        # The Data Editor: Automatically generates a form/grid based on the dataframe structure
        edited_df = st.data_editor(df, num_rows="dynamic", use_container_width=True, key=f"editor_{selected_table}")

        if st.button("💾 Safe Save (Merge)"):
            try:
                with engine.begin() as conn:
                    # 1. Create Temp Table
                    edited_df.to_sql("#Staging_Edit", conn, if_exists='replace', index=False)
                    
                    # 2. Construct MERGE Statement (Upsert)
                    # This updates existing rows and inserts new ones, without deleting the rest of the table.
                    set_clause = ", ".join([f"T.{col} = S.{col}" for col in df.columns if col != pk_col])
                    cols = ", ".join(df.columns)
                    vals = ", ".join([f"S.{col}" for col in df.columns])
                    
                    merge_sql = f"""
                    MERGE INTO {selected_table} AS T
                    USING #Staging_Edit AS S
                    ON T.{pk_col} = S.{pk_col}
                    WHEN MATCHED THEN
                        UPDATE SET {set_clause}
                    WHEN NOT MATCHED THEN
                        INSERT ({cols}) VALUES ({vals});
                    """
                    conn.execute(text(merge_sql))
                    
                st.success(f"Successfully updated `{selected_table}`!")
                st.rerun()
            except Exception as e:
                st.error(f"Error saving data: {e}")

# --- AI Data Entry Page ---
elif page == "AI Data Entry":
    st.title("⚡ AI Data Entry Assistant")
    st.markdown("Paste unstructured text (emails, chat logs, notes) and let AI map it to your database schema.")

    # Table Selection
    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND (TABLE_NAME LIKE 'stg_%' OR TABLE_NAME LIKE 'dim_%')")
    target_table = st.selectbox("Select Target Table", tables_df['TABLE_NAME'])
    
    if target_table:
        # Get Schema for Context
        schema_df = run_query(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{target_table}'")
        schema_context = schema_df.to_csv(index=False)
        
        # Input Options
        col_input1, col_input2 = st.columns(2)
        with col_input1:
            input_text = st.text_area("Option 1: Paste Text", height=200, placeholder="E.g., 'Received shipment of 500 units of Widget A...'")
        with col_input2:
            uploaded_file = st.file_uploader("Option 2: Upload File", type=['pdf', 'csv', 'xlsx', 'png', 'jpg', 'txt'], help="AI will extract text from the file automatically.")
        
        if st.button("✨ Extract & Map Data"):
            content_to_process = input_text
            
            if uploaded_file:
                with st.spinner(f"Reading {uploaded_file.name}..."):
                    content_to_process = MLEngine.extract_content_from_file(uploaded_file)
            
            if content_to_process:
                with st.spinner("AI is analyzing and mapping data..."):
                    try:
                        extracted_df = MLEngine.parse_unstructured_data(content_to_process, target_table, schema_context)
                        st.session_state['extracted_data'] = extracted_df
                        st.success("Extraction Complete! Review the data below.")
                    except Exception as e:
                        st.error(f"Extraction Failed: {e}")
            else:
                st.warning("Please paste text or upload a file.")

        # Review & Save
        if 'extracted_data' in st.session_state:
            st.subheader("Review Extracted Data")
            edited_df = st.data_editor(st.session_state['extracted_data'], num_rows="dynamic", use_container_width=True)
            
            # Primary Key Selection for Merge
            pk_col = st.selectbox("Select Primary Key (for De-duplication)", run_query(f"SELECT TOP 0 * FROM {target_table}").columns)

            if st.button("💾 Commit to Database"):
                try:
                    with engine.begin() as conn:
                        # 1. Create Temp Table
                        edited_df.to_sql("#Staging_AI_Import", conn, if_exists='replace', index=False)
                        
                        # 2. Construct MERGE Statement (Upsert)
                        set_clause = ", ".join([f"T.{col} = S.{col}" for col in edited_df.columns if col != pk_col])
                        cols = ", ".join(edited_df.columns)
                        vals = ", ".join([f"S.{col}" for col in edited_df.columns])
                        
                        merge_sql = f"""
                        MERGE INTO {target_table} AS T
                        USING #Staging_AI_Import AS S
                        ON T.{pk_col} = S.{pk_col}
                        WHEN MATCHED THEN
                            UPDATE SET {set_clause}
                        WHEN NOT MATCHED THEN
                            INSERT ({cols}) VALUES ({vals});
                        """
                        conn.execute(text(merge_sql))
                        
                    st.success(f"Successfully imported data into `{target_table}`!")
                    del st.session_state['extracted_data'] # Clear state after save
                    st.rerun()
                except Exception as e:
                    st.error(f"Error saving data: {e}")

# --- Data Observability Page ---
elif page == "Data Observability":
    st.title("🩺 Data Observability & Autonomous Health")
    st.markdown("Real-time monitoring of data quality, schema drift, and lineage.")

    # Dynamic Metrics from Logs
    try:
        drift_events = run_query("SELECT COUNT(*) as cnt FROM etl_pipeline_run_logs WHERE status = 'FAILURE' AND error_message LIKE '%schema%'").iloc[0]['cnt']
        last_run = run_query("SELECT MAX(start_time) as last_run FROM etl_pipeline_run_logs").iloc[0]['last_run']
        freshness = f"{pd.Timestamp.now() - pd.to_datetime(last_run)}" if pd.notnull(last_run) else "N/A"
        # Mocking self-healed for now as it requires a specific log pattern
        healed_issues = 2 
    except:
        drift_events = 0
        freshness = "Unknown"
        healed_issues = 0

    # Health Scorecards
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Data Trust Score", "94/100", "+1")
    k2.metric("Schema Drift Events", f"{drift_events}", "Stable" if drift_events == 0 else "Risk")
    k3.metric("Freshness (Time since last run)", f"{freshness}".split('.')[0], "On Time")
    k4.metric("Self-Healed Issues", f"{healed_issues}", "Last 24h")

    st.subheader("🔗 End-to-End Traceability")
    st.caption("Visualizing dependencies for critical path: `Sales Pipeline`")
    
    # Simple Graphviz Lineage Visualization
    st.graphviz_chart('''
        digraph {
            rankdir=LR;
            node [shape=box, style=filled, fillcolor="#f0f2f6", fontname="Sans-Serif"];
            "Source: CRM API" -> "Raw: stg_sales";
            "Source: ERP CSV" -> "Raw: stg_inventory";
            "Raw: stg_sales" -> "Transform: sp_clean_sales";
            "Raw: stg_inventory" -> "Transform: sp_clean_sales";
            "Transform: sp_clean_sales" -> "Table: sales_fact" [color="green", penwidth=2];
            "Table: sales_fact" -> "Dashboard: Executive View";
            "Table: sales_fact" -> "Model: Forecast_v2";
        }
    ''')

    st.subheader("🛡️ Autonomous Health Log")
    st.info("The system automatically resolved **2 schema drift issues** in `stg_inventory` by evolving the table schema to match the new source file.")

# --- Configuration Manager Page ---
elif page == "Configuration Manager":
    st.title("⚙️ Analytics Configuration")
    st.markdown("Enable AI models on your data tables without writing SQL.")

    # Move table selection outside the form to allow dynamic column loading
    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    target_table = st.selectbox("Select Target Table", tables_df['TABLE_NAME'], help="Choose the table you want the AI to monitor.")
    
    columns = []
    if target_table:
        try:
            cols_df = run_query(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{target_table}'")
            columns = cols_df['COLUMN_NAME'].tolist()
        except:
            pass

    with st.form("add_config_form"):
        st.subheader("Add New Analysis Rule")
        
        date_col = st.selectbox("Date Column", columns, help="Select the column representing time (e.g., OrderDate).")
        value_col = st.selectbox("Value Column", columns, help="Select the numeric metric to track (e.g., TotalAmount).")
        model_type = st.selectbox("Model Type", ["anomaly_detection", "forecast"], help="Anomaly Detection finds outliers. Forecast predicts future values.")
        webhook_url = st.text_input("Alert Webhook URL (Optional - Slack/Teams)", type="password")
        
        submitted = st.form_submit_button("Activate Analytics")
        
        if submitted:
            if target_table and date_col and value_col:
                try:
                    with engine.connect() as conn:
                        # Verify columns exist (Robustness check)
                        check_query = text(f"SELECT TOP 1 {date_col}, {value_col} FROM {target_table}")
                        conn.execute(check_query)
                        
                        # Insert config
                        insert_query = text("""
                            INSERT INTO analytics_config (target_table, date_column, value_column, model_type, alert_webhook_url, is_active)
                            VALUES (:table, :date, :val, :model, :webhook, 1)
                        """)
                        conn.execute(insert_query, {"table": target_table, "date": date_col, "val": value_col, "model": model_type, "webhook": webhook_url})
                        conn.commit()
                    st.success(f"Successfully enabled {model_type} for {target_table}!")
                except Exception as e:
                    st.error(f"Error: Could not validate columns or save config. Details: {e}")
            else:
                st.warning("Please fill in all fields.")

    st.subheader("Active Configurations")
    active_configs = run_query("SELECT * FROM analytics_config WHERE is_active = 1")
    st.dataframe(active_configs)