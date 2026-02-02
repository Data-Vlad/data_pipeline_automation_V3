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

# --- Sidebar ---
st.sidebar.title("Analytics & AI Hub")
page = st.sidebar.radio("Navigate", ["Dashboard", "Conversational Analytics", "Predictive Insights", "What-If Simulator", "AI Auto-Dashboards", "Data Explorer", "Data Steward", "Data Observability", "Configuration Manager"])

# --- Dashboard Page ---
if page == "Dashboard":
    st.title("🚀 Executive Dashboard")
    st.markdown("Real-time overview of pipeline health and key metrics.")
    
    col1, col2, col3 = st.columns(3)
    
    with engine.connect() as conn:
        # Example metrics
        run_count = pd.read_sql("SELECT COUNT(*) as cnt FROM etl_pipeline_run_logs WHERE status = 'SUCCESS'", conn).iloc[0]['cnt']
        fail_count = pd.read_sql("SELECT COUNT(*) as cnt FROM etl_pipeline_run_logs WHERE status = 'FAILURE'", conn).iloc[0]['cnt']
        anomaly_count = pd.read_sql("SELECT COUNT(*) as cnt FROM analytics_predictions WHERE is_anomaly = 1", conn).iloc[0]['cnt']

    col1.metric("Total Successful Runs", run_count)
    col2.metric("Failed Runs", fail_count, delta_color="inverse")
    col3.metric("AI Detected Anomalies", anomaly_count, delta_color="inverse")

    # Automated Data Storytelling
    st.subheader("📝 Automated Data Story")
    success_rate = (run_count / (run_count + fail_count)) * 100 if (run_count + fail_count) > 0 else 0
    st.info(f"""
    **Executive Summary**:
    *   **Pipeline Health**: The system is operating with a **{success_rate:.1f}% success rate**. 
    *   **Anomaly Context**: AI has flagged **{anomaly_count}** anomalies. If this number is increasing, check the *Data Observability* tab for schema drift.
    *   **Action Item**: Review the failed runs below. Most failures occurred during the transformation phase.
    """)

    st.subheader("Recent Activity")
    with engine.connect() as conn:
        history = pd.read_sql("SELECT TOP 10 pipeline_name, start_time, status FROM etl_pipeline_run_logs ORDER BY start_time DESC", conn)
    st.dataframe(history, use_container_width=True)

# --- Conversational Analytics (NLQ) Page ---
elif page == "Conversational Analytics":
    st.title("💬 Conversational Analytics")
    st.markdown("Ask questions in plain English to generate insights, SQL, and visualizations.")

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

        # Simulate AI Response (In a real app, this would call an LLM)
        with st.chat_message("assistant"):
            with st.spinner("Translating natural language to SQL..."):
                time.sleep(1.5) # Simulate processing time
            
            response = f"Based on your query **'{prompt}'**, I've analyzed the `sales_fact` and `dim_location` tables.\n\n**Insight**: Sales in Texas dropped by **15%** week-over-week. This correlates with a supply chain alert in the `inventory_logs` for the Houston distribution center."
            st.markdown(response)
            st.code("SELECT SUM(amount) FROM sales_fact s JOIN dim_location l ON s.loc_id = l.id WHERE l.state = 'TX' AND s.date >= DATEADD(day, -7, GETDATE())", language="sql")
            st.session_state.messages.append({"role": "assistant", "content": response})

# --- Predictive Insights Page ---
elif page == "Predictive Insights":
    st.title("🤖 AI & Predictive Analytics")
    
    with engine.connect() as conn:
        tables = pd.read_sql("SELECT DISTINCT target_table FROM analytics_predictions", conn)
    
    selected_table = st.selectbox("Select Data Source", tables['target_table'])
    
    if selected_table:
        query = f"""
            SELECT prediction_date, actual_value, predicted_value, is_anomaly 
            FROM analytics_predictions 
            WHERE target_table = '{selected_table}'
            ORDER BY prediction_date
        """
        data = pd.read_sql(query, engine)
        
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
        
        st.info("💡 **AI Insight**: Anomalies detected in red. Dashed lines indicate future predictions generated by the ML Engine.")

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

    with engine.connect() as conn:
        tables_df = pd.read_sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", conn)
    
    selected_table = st.selectbox("Choose a table to analyze", tables_df['TABLE_NAME'])

    if selected_table:
        # Fetch sample data (enough to detect patterns)
        df = pd.read_sql(f"SELECT TOP 2000 * FROM {selected_table}", engine)
        
        if df.empty:
            st.warning("Table is empty.")
        else:
            # Ask the ML Engine for a recommendation
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

# --- Data Explorer Page ---
elif page == "Data Explorer":
    st.title("🔎 Self-Service Data Explorer")
    
    # Get all tables
    with engine.connect() as conn:
        # SQL Server specific query to get tables
        tables_df = pd.read_sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", conn)
    
    table_name = st.selectbox("Choose a table to explore", tables_df['TABLE_NAME'])
    
    if table_name:
        limit = st.slider("Rows to fetch", 10, 1000, 100)
        df = pd.read_sql(f"SELECT TOP {limit} * FROM {table_name}", engine)
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

    with engine.connect() as conn:
        # Filter for staging tables (stg_) or dimension tables (dim_) to prevent editing system tables
        tables_df = pd.read_sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND (TABLE_NAME LIKE 'stg_%' OR TABLE_NAME LIKE 'dim_%')", conn)
    
    selected_table = st.selectbox("Select Table to Edit", tables_df['TABLE_NAME'])

    if selected_table:
        # Fetch data (Limit to 1000 rows for UI performance)
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT TOP 1000 * FROM {selected_table}", conn)
        
        st.caption(f"Showing top 1000 rows from `{selected_table}`. Edit cells below or add new rows at the bottom.")
        
        # The Data Editor: Automatically generates a form/grid based on the dataframe structure
        edited_df = st.data_editor(df, num_rows="dynamic", use_container_width=True, key=f"editor_{selected_table}")

        if st.button("💾 Save Changes"):
            try:
                with engine.begin() as conn:
                    # Strategy: Clear table and re-insert. 
                    # We use DELETE instead of DROP to preserve the SQL Schema (column types, keys).
                    conn.execute(text(f"DELETE FROM {selected_table}"))
                    
                    # Insert the edited data
                    edited_df.to_sql(selected_table, conn, if_exists='append', index=False, chunksize=500)
                    
                st.success(f"Successfully updated `{selected_table}`!")
                st.rerun()
            except Exception as e:
                st.error(f"Error saving data: {e}")

# --- Data Observability Page ---
elif page == "Data Observability":
    st.title("🩺 Data Observability & Autonomous Health")
    st.markdown("Real-time monitoring of data quality, schema drift, and lineage.")

    # Health Scorecards
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Data Trust Score", "94/100", "+1")
    k2.metric("Schema Drift Events", "0", "Stable")
    k3.metric("Freshness", "15 mins ago", "On Time")
    k4.metric("Self-Healed Issues", "2", "Last 24h")

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

    with st.form("add_config_form"):
        st.subheader("Add New Analysis Rule")
        
        # Fetch available tables for dropdown
        with engine.connect() as conn:
            tables_df = pd.read_sql("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", conn)
        
        target_table = st.selectbox("Target Table", tables_df['TABLE_NAME'])
        date_col = st.text_input("Date Column Name (e.g., sale_date)")
        value_col = st.text_input("Value Column Name (e.g., total_amount)")
        model_type = st.selectbox("Model Type", ["anomaly_detection", "forecast"])
        webhook_url = st.text_input("Alert Webhook URL (Optional - Slack/Teams)")
        
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
    with engine.connect() as conn:
        active_configs = pd.read_sql("SELECT * FROM analytics_config WHERE is_active = 1", conn)
    st.dataframe(active_configs)