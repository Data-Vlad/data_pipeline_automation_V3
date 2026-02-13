import streamlit as st
import pandas as pd
from utils import run_query, MLEngine, engine

def page_semantic_search():
    st.title("🧠 Semantic Search (RAG)")
    st.markdown("Search your data using **meaning**, not just keywords.")

    tables_df = run_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    selected_table = st.selectbox("Select Table", tables_df['TABLE_NAME'])

    if selected_table:
        df_preview = run_query(f"SELECT TOP 5 * FROM {selected_table}")
        text_cols = df_preview.select_dtypes(include=['object']).columns.tolist()
        search_col = st.selectbox("Column to Search", text_cols)
        query = st.text_input("Search Query", placeholder="e.g., 'Customers complaining about late delivery'")

        if st.button("🔍 Search") and query:
            full_df = run_query(f"SELECT TOP 200 * FROM {selected_table}")
            with st.spinner("Generating embeddings..."):
                results = MLEngine.perform_semantic_search(full_df, query, search_col)
            
            if not results.empty:
                st.dataframe(results[['similarity_score'] + [c for c in results.columns if c != 'similarity_score']], use_container_width=True)
            else:
                st.warning("No results found or OpenAI API not configured.")

def page_multi_modal_analysis():
    st.title("📷 Multi-Modal Data Extraction")
    st.markdown("Extract structured data from unstructured files (Images, PDFs) using AI.")

    uploaded_file = st.file_uploader("Upload Invoice, Contract, or Image", type=['png', 'jpg', 'jpeg', 'pdf'])
    extraction_schema = st.text_area("Fields to Extract", "Invoice Number, Date, Total Amount, Vendor Name")

    if uploaded_file and st.button("Extract Data"):
        file_bytes = uploaded_file.getvalue()
        file_type = uploaded_file.name.split('.')[-1].lower()
        
        with st.spinner("Analyzing file content..."):
            result = MLEngine.extract_structured_data(file_bytes, file_type, extraction_schema)
        
        if "error" in result:
            st.error(result["error"])
        else:
            st.success("Extraction Complete!")
            st.json(result)
            df_result = pd.DataFrame([result])
            st.dataframe(df_result)