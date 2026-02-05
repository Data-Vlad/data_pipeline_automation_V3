import streamlit as st
import pandas as pd
import io
import numpy as np

def create_profile_df(df: pd.DataFrame) -> pd.DataFrame:
    """Generates a data profile of the DataFrame."""
    profile = {
        "Column Name": [],
        "Data Type": [],
        "Missing Values": [],
        "% Missing": [],
        "Unique Values": [],
        "Sample Value": [],
    }
    for col in df.columns:
        profile["Column Name"].append(col)
        profile["Data Type"].append(df[col].dtype)
        missing_count = df[col].isnull().sum()
        profile["Missing Values"].append(missing_count)
        profile["% Missing"].append(f"{(missing_count / len(df) * 100):.2f}%")
        profile["Unique Values"].append(df[col].nunique())
        
        first_valid_index = df[col].first_valid_index()
        if first_valid_index is not None:
            profile["Sample Value"].append(df[col].loc[first_valid_index])
        else:
            profile["Sample Value"].append(None)
            
    return pd.DataFrame(profile)

def page_file_converter():
    st.title("🔄 Self-Service File Converter")
    st.markdown("""
    **Utility:** Convert, profile, and transform data files instantly without writing code.
    **Supported Formats:** CSV, Excel (XLSX), JSON, Parquet.
    """)

    uploaded_file = st.file_uploader("Upload Source File", type=["csv", "xlsx", "xls", "json", "parquet"])

    if uploaded_file:
        # Use a unique ID for the file to manage session state
        file_id = f"{uploaded_file.name}-{uploaded_file.size}"
        
        # Initialize or Reset Session State if a new file is uploaded
        if "converter_df" not in st.session_state or st.session_state.get("converter_file_id") != file_id:
            try:
                file_ext = uploaded_file.name.split('.')[-1].lower()
                df = None
                if file_ext == 'csv':
                    df = pd.read_csv(uploaded_file)
                elif file_ext in ['xlsx', 'xls']:
                    df = pd.read_excel(uploaded_file)
                elif file_ext == 'json':
                    df = pd.read_json(uploaded_file)
                elif file_ext == 'parquet':
                    df = pd.read_parquet(uploaded_file)
                
                if df is not None:
                    st.session_state.converter_df = df
                    st.session_state.original_df = df.copy() # Keep a copy for reset
                    st.session_state.converter_file_id = file_id
            except Exception as e:
                st.error(f"Error reading file: {str(e)}")
                if "converter_df" in st.session_state:
                    del st.session_state.converter_df
                return

        if "converter_df" in st.session_state:
            df = st.session_state.converter_df
            
            st.success(f"✅ Loaded **{uploaded_file.name}** | Current data shape: **{df.shape[0]}** rows, **{df.shape[1]}** columns")
            
            if st.button("Reset All Transformations"):
                st.session_state.converter_df = st.session_state.original_df.copy()
                st.rerun()

            # --- Profiling Expander ---
            with st.expander("📊 Data Profile & Quality", expanded=False):
                st.info("Get a quick overview of your data's structure and quality.")
                profile_df = create_profile_df(df)
                st.dataframe(profile_df, use_container_width=True)

            # --- Shaping Expander ---
            with st.expander("🔪 Data Shaping (Filter & Select)", expanded=False):
                tab1, tab2 = st.tabs(["Select Columns", "Filter Rows"])

                with tab1:
                    st.markdown("##### Select Columns to Keep")
                    all_columns = list(st.session_state.original_df.columns)
                    selected_columns = st.multiselect(
                        "Choose columns",
                        options=all_columns,
                        default=list(df.columns)
                    )
                    if st.button("Apply Column Selection", use_container_width=True):
                        st.session_state.converter_df = st.session_state.converter_df[selected_columns]
                        st.rerun()

                with tab2:
                    st.markdown("##### Filter Rows by Condition")
                    filter_col = st.selectbox("Column to filter", options=df.columns)
                    
                    if pd.api.types.is_numeric_dtype(df[filter_col]):
                        operators = ['==', '!=', '>', '<', '>=', '<=']
                    else:
                        operators = ['equals', 'not equals', 'contains', 'starts with', 'ends with']
                    
                    operator = st.selectbox("Operator", options=operators)
                    value = st.text_input("Value", help="For 'contains' on text, this is case-sensitive.")

                    if st.button("Apply Filter", use_container_width=True):
                        try:
                            filtered_df = st.session_state.converter_df.copy()
                            if pd.api.types.is_numeric_dtype(df[filter_col]):
                                val = float(value)
                                if operator == '==': filtered_df = filtered_df[filtered_df[filter_col] == val]
                                elif operator == '!=': filtered_df = filtered_df[filtered_df[filter_col] != val]
                                elif operator == '>': filtered_df = filtered_df[filtered_df[filter_col] > val]
                                elif operator == '<': filtered_df = filtered_df[filtered_df[filter_col] < val]
                                elif operator == '>=': filtered_df = filtered_df[filtered_df[filter_col] >= val]
                                elif operator == '<=': filtered_df = filtered_df[filtered_df[filter_col] <= val]
                            else: # String operations
                                val = str(value)
                                if operator == 'equals': filtered_df = filtered_df[filtered_df[filter_col] == val]
                                elif operator == 'not equals': filtered_df = filtered_df[filtered_df[filter_col] != val]
                                elif operator == 'contains': filtered_df = filtered_df[filtered_df[filter_col].str.contains(val, na=False)]
                                elif operator == 'starts with': filtered_df = filtered_df[filtered_df[filter_col].str.startswith(val, na=False)]
                                elif operator == 'ends with': filtered_df = filtered_df[filtered_df[filter_col].str.endswith(val, na=False)]
                            
                            st.session_state.converter_df = filtered_df
                            st.rerun()
                        except Exception as e:
                            st.error(f"Filter failed: {e}")

            # --- Enrichment Expander ---
            with st.expander("🛠️ Enrichment & Cleaning", expanded=False):
                st.info("Add new fields or fill gaps to standardize your data before export.")
                tab1, tab2 = st.tabs(["➕ Add Column", "🩹 Fill Missing Values"])
                
                with tab1:
                    c1, c2, c3 = st.columns([2, 2, 1])
                    new_col_name = c1.text_input("New Column Name", placeholder="e.g., Source_Region")
                    new_col_val = c2.text_input("Constant Value", placeholder="e.g., North_America")
                    if c3.button("Add Field", use_container_width=True):
                        if new_col_name:
                            st.session_state.converter_df[new_col_name] = new_col_val
                            st.success(f"Added column '{new_col_name}'")
                            st.rerun()
                
                with tab2:
                    cols_with_nans = [c for c in df.columns if df[c].hasnans]
                    if cols_with_nans:
                        c1, c2, c3 = st.columns([2, 2, 1])
                        fill_col = c1.selectbox("Select Column with Missing Data", cols_with_nans)
                        fill_val = c2.text_input("Fill Value", placeholder="0 or Unknown")
                        if c3.button("Apply Fill", use_container_width=True):
                            st.session_state.converter_df[fill_col] = st.session_state.converter_df[fill_col].fillna(fill_val)
                            st.success(f"Filled missing values in '{fill_col}'")
                            st.rerun()
                    else:
                        st.write("No columns with missing values detected.")

            # --- Preview & Download ---
            st.divider()
            st.subheader("Preview of Transformed Data")
            st.dataframe(df.head(), use_container_width=True)
            
            st.subheader("Download Converted File")
            col1, col2 = st.columns(2)
            with col1:
                target_format = st.selectbox("Select Output Format", ["CSV", "Excel", "JSON", "Parquet"])
            
            with col2:
                output_buffer = io.BytesIO()
                new_filename = uploaded_file.name.rsplit('.', 1)[0]
                
                try:
                    if target_format == "CSV":
                        df.to_csv(output_buffer, index=False)
                        mime_type = "text/csv"
                        new_filename += ".csv"
                    elif target_format == "Excel":
                        with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                            df.to_excel(writer, index=False)
                        mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        new_filename += ".xlsx"
                    elif target_format == "JSON":
                        df.to_json(output_buffer, orient="records", date_format="iso")
                        mime_type = "application/json"
                        new_filename += ".json"
                    elif target_format == "Parquet":
                        df.to_parquet(output_buffer, index=False)
                        mime_type = "application/octet-stream"
                        new_filename += ".parquet"
                    
                    st.download_button(
                        label=f"⬇️ Download {new_filename}",
                        data=output_buffer.getvalue(),
                        file_name=new_filename,
                        mime=mime_type,
                        use_container_width=True,
                        type="primary"
                    )
                except Exception as e:
                    st.error(f"Conversion failed: {str(e)}")