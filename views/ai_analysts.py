import streamlit as st
import pandas as pd
import plotly.express as px
import time
import os
from utils import run_query, MLEngine

def page_conversational_analytics():
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

def page_agentic_analyst():
    st.title("🕵️ Agentic Analyst")
    st.markdown("An autonomous agent that plans and executes multi-step data analysis tasks to achieve high-level goals.")

    if not os.getenv("OPENAI_API_KEY"):
        st.warning("⚠️ OpenAI API Key not found. Please set `OPENAI_API_KEY` in .env.")
        st.stop()

    goal = st.text_area("Define your analysis goal:", placeholder="e.g., Analyze the sales trend for the last 3 months...", height=100)
    
    if st.button("🚀 Launch Agent"):
        if not goal:
            st.warning("Please define a goal for the agent.")
        else:
            st.info(f"**Goal:** {goal}")
            
            with st.status("🧠 Agent is planning...", expanded=True) as status:
                st.write("Analyzing request intent...")
                time.sleep(1)
                st.write("Identifying relevant data tables...")
                time.sleep(0.5)
                st.write("Formulating execution strategy: `SQL Retrieval` -> `Data Summarization`")
                status.update(label="Plan confirmed!", state="complete", expanded=False)
            
            with st.spinner("🕵️ Agent is working..."):
                try:
                    schema_df = run_query("SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME NOT LIKE 'sys%'")
                    schema_context = schema_df.to_csv(index=False)
                    
                    st.subheader("1. Data Retrieval")
                    sql_query = MLEngine.generate_sql_from_question(goal, schema_context)
                    st.code(sql_query, language="sql")
                    
                    df = run_query(sql_query)
                    st.dataframe(df, use_container_width=True)
                    
                    st.subheader("2. Analysis & Insights")
                    if not df.empty:
                        metrics = {}
                        if len(df) > 0:
                            numeric_cols = df.select_dtypes(include=['number']).columns
                            for col in numeric_cols:
                                metrics[f"avg_{col}"] = df[col].mean()
                                metrics[f"total_{col}"] = df[col].sum()
                            metrics["row_count"] = len(df)
                        
                        story = MLEngine.generate_data_story(metrics, context_str=f"Analysis for goal: {goal}")
                        st.markdown(f"### 📋 Final Report\n{story}")
                    else:
                        st.warning("The agent completed the query but found no matching data to analyze.")
                except Exception as e:
                    st.error(f"Agent encountered an error during execution: {e}")

def page_ai_auto_dashboards():
    st.title("✨ AI Auto-Dashboards")
    st.markdown("Select any table, and the system will **automatically decide** the best way to visualize it.")

    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    selected_table = st.selectbox("Choose a table to analyze", tables_df['TABLE_NAME'])

    if selected_table:
        cols_df = run_query(f"SELECT TOP 0 * FROM {selected_table}")
        all_cols = cols_df.columns.tolist()
        numeric_cols = cols_df.select_dtypes(include=['float', 'int']).columns.tolist()
        date_cols = [c for c in all_cols if 'date' in c.lower() or 'time' in c.lower()]

        if date_cols and numeric_cols:
            date_col = date_cols[0]
            val_col = numeric_cols[0]
            st.info(f"Running optimized aggregation on `{selected_table}`...")
            agg_query = f"SELECT {date_col}, SUM({val_col}) as {val_col} FROM {selected_table} GROUP BY {date_col} ORDER BY {date_col}"
            df = run_query(agg_query)
            rec = MLEngine.recommend_visualization(df)
            st.subheader(rec.get('title', 'Analysis'))
            st.caption(f"🤖 **AI Decision**: {rec.get('reasoning')}")
            
            if rec['type'] == 'time_series':
                df[rec['x']] = pd.to_datetime(df[rec['x']])
                fig = px.line(df, x=rec['x'], y=rec['y'], title=f"{rec['y']} over Time")
                st.plotly_chart(fig, use_container_width=True)
            # ... (Other visualization types can be added here as needed)
        else:
            df = run_query(f"SELECT TOP 1000 * FROM {selected_table}")
            st.dataframe(df)