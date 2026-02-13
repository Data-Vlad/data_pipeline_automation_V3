import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text
from utils import run_query, MLEngine, engine

def page_predictive_insights():
    st.title("🤖 AI & Predictive Analytics")
    tables = run_query("SELECT DISTINCT target_table FROM analytics_predictions")
    selected_table = st.selectbox("Select Data Source", tables['target_table'])
    
    if selected_table:
        query = f"SELECT prediction_date, actual_value, predicted_value, is_anomaly FROM analytics_predictions WHERE target_table = '{selected_table}' ORDER BY prediction_date"
        data = run_query(query)
        
        fig = go.Figure()
        if 'actual_value' in data.columns and data['actual_value'].notna().any():
            fig.add_trace(go.Scatter(x=data['prediction_date'], y=data['actual_value'], mode='lines', name='Actual Value'))
            anomalies = data[data['is_anomaly'] == 1]
            fig.add_trace(go.Scatter(x=anomalies['prediction_date'], y=anomalies['actual_value'], mode='markers', name='Anomaly', marker=dict(color='red', size=10)))

        forecasts = data[data['predicted_value'].notna()]
        if not forecasts.empty:
            fig.add_trace(go.Scatter(x=forecasts['prediction_date'], y=forecasts['predicted_value'], mode='lines', name='AI Forecast', line=dict(dash='dash')))

        st.plotly_chart(fig, use_container_width=True)
        
        if not anomalies.empty:
            st.subheader("🛠️ Anomaly Feedback")
            to_dismiss = st.selectbox("Select an anomaly to dismiss", anomalies['prediction_date'])
            if st.button("Dismiss Selected Anomaly"):
                with engine.begin() as conn:
                    conn.execute(text(f"UPDATE analytics_predictions SET is_anomaly = 0 WHERE target_table = '{selected_table}' AND prediction_date = '{to_dismiss}'"))
                st.success(f"Dismissed anomaly for {to_dismiss}.")
                st.rerun()

def page_root_cause_analysis():
    st.title("📉 Automated Root Cause Analysis")
    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    selected_table = st.selectbox("Select Table", tables_df['TABLE_NAME'])

    if selected_table:
        cols_df = run_query(f"SELECT TOP 0 * FROM {selected_table}")
        all_cols = cols_df.columns.tolist()
        numeric_cols = cols_df.select_dtypes(include=['float', 'int']).columns.tolist()
        date_cols = [c for c in all_cols if 'date' in c.lower() or 'time' in c.lower()]

        col1, col2, col3 = st.columns(3)
        date_col = col1.selectbox("Date Column", date_cols) if date_cols else None
        val_col = col2.selectbox("Metric to Analyze", numeric_cols) if numeric_cols else None
        
        if date_col and val_col:
            ts_df = run_query(f"SELECT {date_col}, SUM({val_col}) as {val_col} FROM {selected_table} GROUP BY {date_col} ORDER BY {date_col}")
            fig = px.line(ts_df, x=date_col, y=val_col, title=f"Trend: {val_col}")
            st.plotly_chart(fig, use_container_width=True)

            dates = ts_df[date_col].dt.date.unique()
            target_date = col3.selectbox("Select Anomaly/Target Date", dates, index=len(dates)-1)
            
            if st.button("🔍 Analyze Drivers"):
                compare_date = target_date - pd.Timedelta(days=1)
                full_data = run_query(f"SELECT * FROM {selected_table} WHERE CAST({date_col} AS DATE) IN ('{target_date}', '{compare_date}')")
                insights = MLEngine.analyze_root_cause(full_data, date_col, val_col, target_date, compare_date)
                
                st.subheader(f"Drivers of Change: {compare_date} vs {target_date}")
                if not insights:
                    st.warning("No significant drivers found.")
                else:
                    for item in insights:
                        delta_color = "inverse" if item['impact'] < 0 else "normal"
                        st.metric(label=f"{item['dimension']} = {item['segment']}", value=f"{item['curr_value']:,.0f}", delta=f"{item['impact']:+,.0f} ({item['contribution_pct']:.1f}%)", delta_color=delta_color)

def page_clustering_segmentation():
    st.title("🧩 Customer & Entity Segmentation")
    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    selected_table = st.selectbox("Select Table", tables_df['TABLE_NAME'])

    if selected_table:
        df_preview = run_query(f"SELECT TOP 5 * FROM {selected_table}")
        numeric_cols = df_preview.select_dtypes(include=['float', 'int']).columns.tolist()
        selected_features = st.multiselect("Select Features", numeric_cols, default=numeric_cols)
        n_clusters = st.slider("Number of Clusters", 2, 10, 3)

        if st.button("Run Clustering") and len(selected_features) >= 2:
            full_df = run_query(f"SELECT {', '.join(selected_features)} FROM {selected_table}")
            with st.spinner("Running K-Means Algorithm..."):
                clustered_df = MLEngine.perform_clustering(full_df, selected_features, n_clusters)
            st.success("Clustering Complete!")
            fig = px.scatter(clustered_df, x=selected_features[0], y=selected_features[1], color='cluster_id')
            st.plotly_chart(fig, use_container_width=True)

def page_prescriptive_optimization():
    st.title("🎯 Prescriptive Analytics & Optimization")
    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    selected_table = st.selectbox("Select Table", tables_df['TABLE_NAME'])

    if selected_table:
        df_preview = run_query(f"SELECT TOP 100 * FROM {selected_table}")
        numeric_cols = df_preview.select_dtypes(include=['float', 'int']).columns.tolist()
        col1, col2 = st.columns(2)
        target_col = col1.selectbox("Target to Maximize", numeric_cols)
        input_cols = col2.multiselect("Controllable Inputs", [c for c in numeric_cols if c != target_col])

        if st.button("🚀 Run Optimization") and target_col and input_cols:
            full_df = run_query(f"SELECT {target_col}, {', '.join(input_cols)} FROM {selected_table}")
            with st.spinner("Solving optimization problem..."):
                result = MLEngine.optimize_business_objective(full_df, target_col, input_cols)
            
            if result.get("success"):
                st.success("Optimization Solved!")
                st.metric("Projected Maximum Target", f"{result['optimized_target']:,.2f}")
                rec_df = pd.DataFrame(list(result['recommendations'].items()), columns=["Input Variable", "Recommended Value"])
                st.dataframe(rec_df, use_container_width=True)
            else:
                st.error(f"Optimization failed: {result.get('message')}")

def page_what_if_simulator():
    st.title("🎛️ What-If Scenario Analysis")
    st.markdown("Simulate business outcomes by tweaking key variables.")

    col1, col2 = st.columns([1, 3])
    with col1:
        st.subheader("Scenario Parameters")
        price_adj = st.slider("Pricing Adjustment (%)", -20, 20, 0)
        marketing_spend = st.slider("Marketing Spend Increase (%)", 0, 50, 10)
        churn_rate = st.slider("Expected Churn Rate (%)", 0, 10, 2)

    with col2:
        st.subheader("Projected Impact: Monthly Revenue")
        base_revenue = 150000
        impact_factor = 1 + (price_adj * 0.015) + (marketing_spend * 0.008) - (churn_rate * 0.02)
        projected_revenue = base_revenue * impact_factor
        delta = projected_revenue - base_revenue

        st.metric("Projected Revenue", f"${projected_revenue:,.0f}", f"{delta:+,.0f}", delta_color="normal")

        months = ["Month 1", "Month 2", "Month 3", "Month 4", "Month 5", "Month 6"]
        baseline_trend = [base_revenue * (1 + 0.01*i) for i in range(6)]
        scenario_trend = [val * impact_factor for val in baseline_trend]

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=months, y=baseline_trend, name='Baseline Forecast', line=dict(dash='dot', color='gray')))
        fig.add_trace(go.Scatter(x=months, y=scenario_trend, name='Simulated Scenario', fill='tonexty', line=dict(color='#00CC96')))
        st.plotly_chart(fig, use_container_width=True)