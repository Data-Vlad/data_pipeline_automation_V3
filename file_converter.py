import streamlit as st
import pandas as pd
import io
import numpy as np

# Try to import MLEngine for advanced features
try:
    from elt_project.core.ml_engine import MLEngine
    MLENGINE_AVAILABLE = True
except ImportError:
    MLENGINE_AVAILABLE = False
    # Define a dummy class if the import fails so the app doesn't crash
    class MLEngine:
        @staticmethod
        def suggest_data_repairs(df: pd.DataFrame) -> list:
            return []

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
    **Utility:** Convert, profile, and transform data files instantly. Supports structured files (CSV, Excel, etc.) and unstructured documents (Images, PDF) via AI extraction.
    """)

    uploaded_file = st.file_uploader(
        "Upload Source File", 
        type=["csv", "xlsx", "xls", "json", "parquet", "png", "jpg", "jpeg", "pdf"],
        help="Upload structured data or an image/PDF of a document to extract data from."
    )

    if uploaded_file:
        # Use a unique ID for the file to manage session state
        file_id = f"{uploaded_file.name}-{uploaded_file.size}"
        file_ext = uploaded_file.name.split('.')[-1].lower()
        is_unstructured = file_ext in ['png', 'jpg', 'jpeg', 'pdf']
        
        # Initialize or Reset Session State if a new file is uploaded
        if st.session_state.get("converter_file_id") != file_id:
            # Clear all previous state for a new file
            for key in list(st.session_state.keys()):
                if key.startswith("converter_") or key.startswith("ai_"):
                    del st.session_state[key]
            st.session_state.converter_file_id = file_id

        # --- Main Logic Flow ---
        # 1. If it's an unstructured file and not yet processed, show the AI extraction UI.
        if is_unstructured and "converter_df" not in st.session_state:
            st.info(f"🧠 Detected unstructured file: **{uploaded_file.name}**. Use the AI extractor to convert it to a table.")
            
            with st.form("unstructured_extraction_form"):
                st.subheader("🤖 AI-Powered Document Extraction")
                
                if file_ext in ['png', 'jpg', 'jpeg']:
                    st.image(uploaded_file, width=300)
                
                schema_help = """
                Define the data you want to extract as a JSON object. 
                For example, to extract fields from an invoice: 
                `{"invoice_id": "string", "customer_name": "string", "total_amount": "number", "items": [{"name": "string", "quantity": "integer"}]}`
                """
                extraction_schema = st.text_area(
                    "Extraction Schema (JSON)", 
                    height=150, 
                    help=schema_help,
                    value='{\n  "invoice_number": "The invoice ID",\n  "total_amount": "The final total amount"\n}'
                )
                
                submitted = st.form_submit_button("Extract Structured Data", use_container_width=True, type="primary")

                if submitted:
                    with st.spinner("🤖 AI is reading the document..."):
                        try:
                            file_bytes = uploaded_file.getvalue()
                            result = MLEngine.extract_structured_data(
                                file_bytes=file_bytes,
                                file_type=file_ext,
                                extraction_schema=extraction_schema
                            )
                            
                            if "error" in result:
                                st.error(f"Extraction Failed: {result['error']}")
                            else:
                                # Handle single object or list of objects
                                if isinstance(result, dict) and all(isinstance(v, list) for v in result.values()):
                                    df = pd.DataFrame(result)
                                elif isinstance(result, list):
                                    df = pd.json_normalize(result)
                                else:
                                    df = pd.DataFrame([result])

                                st.session_state.converter_df = df
                                st.session_state.original_df = df.copy()
                                st.rerun()

                        except Exception as e:
                            st.error(f"An error occurred during extraction: {e}")
            return # Stop further execution until data is extracted

        # 2. If it's a structured file, load it directly (only on first upload)
        elif not is_unstructured and "converter_df" not in st.session_state:
            try:
                df = None
                if file_ext == 'csv': df = pd.read_csv(uploaded_file)
                elif file_ext in ['xlsx', 'xls']: df = pd.read_excel(uploaded_file)
                elif file_ext == 'json': df = pd.read_json(uploaded_file)
                elif file_ext == 'parquet': df = pd.read_parquet(uploaded_file)
                
                st.session_state.converter_df = df
                st.session_state.original_df = df.copy()
            except Exception as e:
                st.error(f"Error reading file: {e}")
                return
        
        # 3. If a DataFrame exists in the session, show the main transformation UI
        if "converter_df" in st.session_state:
            df = st.session_state.converter_df
            
            st.success(f"✅ Loaded **{uploaded_file.name}** | Current data shape: **{df.shape[0]}** rows, **{df.shape[1]}** columns")
            
            # --- AI Summary Expander ---
            with st.expander("🤖 AI Summary & Insights", expanded=False):
                st.write("Get a quick, natural language summary of your data's key characteristics.")
                if st.button("Generate AI Summary", key="gen_ai_summary"):
                    with st.spinner("🧠 Analyzing data and generating summary..."):
                        profile = create_profile_df(df)
                        metrics = {
                            "num_rows": len(df), "num_cols": len(df.columns),
                            "missing_values_overview": profile[["Column Name", "% Missing"]].to_dict('records'),
                            "column_data_types": profile[["Column Name", "Data Type"]].astype(str).to_dict('records')
                        }
                        summary = MLEngine.generate_data_story(metrics, context_str=f"This data is from a file named {uploaded_file.name}.")
                        st.session_state.ai_summary = summary
                
                if "ai_summary" in st.session_state:
                    st.markdown(st.session_state.ai_summary)

            if st.button("Reset All Transformations"):
                st.session_state.converter_df = st.session_state.original_df.copy()
                # Clear any cached suggestions
                for key in list(st.session_state.keys()):
                    if key.startswith("ai_") or key.startswith("repair_"):
                        del st.session_state[key]
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

            # --- Transformations Expander ---
            with st.expander("🛠️ Transformations & Cleaning", expanded=False):
                st.info("Rename, re-type, clean, and enrich your data before export.")

                tab_list = [
                    "🔄 Rename & Re-type",
                    "➕ Add & Fill",
                    "🧹 Clean & Deduplicate",
                    "📊 Reshape & Aggregate"
                ]
                if MLENGINE_AVAILABLE:
                    tab_list.append("🤖 AI Repair Suggestions")

                tabs = st.tabs(tab_list)

                # Tab 1: Rename & Re-type
                with tabs[0]:
                    st.markdown("##### Rename Column")
                    c1, c2, c3 = st.columns([2, 2, 1])
                    col_to_rename = c1.selectbox("Column to rename", options=df.columns, key="rename_col")
                    new_col_name_input = c2.text_input("New column name", key="new_col_name")
                    if c3.button("Rename", use_container_width=True):
                        if new_col_name_input and col_to_rename:
                            st.session_state.converter_df.rename(columns={col_to_rename: new_col_name_input}, inplace=True)
                            st.rerun()

                    st.divider()
                    st.markdown("##### Change Column Data Type")
                    c1, c2, c3 = st.columns([2, 2, 1])
                    col_to_retype = c1.selectbox("Column to convert", options=df.columns, key="retype_col")
                    target_type = c2.selectbox("Target data type", ["string", "integer", "float", "datetime"])

                    if c3.button("Convert", use_container_width=True):
                        try:
                            current_series = st.session_state.converter_df[col_to_retype]
                            if target_type == "string":
                                st.session_state.converter_df[col_to_retype] = current_series.astype(str)
                            elif target_type == "integer":
                                st.session_state.converter_df[col_to_retype] = pd.to_numeric(current_series, errors='coerce').astype('Int64')
                            elif target_type == "float":
                                st.session_state.converter_df[col_to_retype] = pd.to_numeric(current_series, errors='coerce').astype(float)
                            elif target_type == "datetime":
                                st.session_state.converter_df[col_to_retype] = pd.to_datetime(current_series, errors='coerce')
                            st.success(f"Converted '{col_to_retype}' to {target_type}.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Conversion failed: {e}")

                # Tab 2: Add & Fill
                with tabs[1]:
                    st.markdown("##### ➕ Add Column with Constant Value")
                    c1, c2, c3 = st.columns([2, 2, 1])
                    new_col_name = c1.text_input("New Column Name", placeholder="e.g., Source_Region", key="add_col_name")
                    new_col_val = c2.text_input("Constant Value", placeholder="e.g., North_America", key="add_col_val")
                    if c3.button("Add Field", use_container_width=True, key="add_field_btn"):
                        if new_col_name:
                            st.session_state.converter_df[new_col_name] = new_col_val
                            st.success(f"Added column '{new_col_name}'")
                            st.rerun()

                    st.divider()
                    st.markdown("##### 🩹 Fill Missing Values")
                    cols_with_nans = [c for c in df.columns if df[c].hasnans]
                    if cols_with_nans:
                        c1, c2, c3 = st.columns([2, 2, 1])
                        fill_col = c1.selectbox("Select Column with Missing Data", cols_with_nans, key="fill_col")
                        fill_val = c2.text_input("Fill Value", placeholder="0 or Unknown", key="fill_val")
                        if c3.button("Apply Fill", use_container_width=True, key="fill_btn"):
                            try:
                                if pd.api.types.is_numeric_dtype(df[fill_col]):
                                    fill_val = float(fill_val)
                            except ValueError:
                                pass  # Keep as string if cast fails
                            st.session_state.converter_df[fill_col] = st.session_state.converter_df[fill_col].fillna(fill_val)
                            st.success(f"Filled missing values in '{fill_col}'.")
                            st.rerun()
                    else:
                        st.write("No columns with missing values detected.")

                # Tab 3: Clean & Deduplicate
                with tabs[2]:
                    st.markdown("##### Drop Duplicate Rows")
                    subset_cols = st.multiselect(
                        "Consider subset of columns (optional)",
                        options=df.columns,
                        help="If no columns are selected, all columns will be used to identify duplicates."
                    )
                    if st.button("Drop Duplicates", use_container_width=True):
                        subset = subset_cols if subset_cols else None
                        original_rows = len(st.session_state.converter_df)
                        st.session_state.converter_df.drop_duplicates(subset=subset, inplace=True)
                        new_rows = len(st.session_state.converter_df)
                        st.success(f"Removed {original_rows - new_rows} duplicate rows.")
                        st.rerun()

                    st.divider()
                    st.markdown("##### Trim Whitespace from Text Columns")
                    string_cols = df.select_dtypes(include=['object']).columns.tolist()
                    if string_cols:
                        cols_to_trim = st.multiselect("Select text columns to trim", options=string_cols)
                        if st.button("Trim Whitespace", use_container_width=True):
                            for col in cols_to_trim:
                                if st.session_state.converter_df[col].dtype == 'object':
                                    st.session_state.converter_df[col] = st.session_state.converter_df[col].str.strip()
                            st.success(f"Trimmed whitespace from {len(cols_to_trim)} column(s).")
                            st.rerun()
                    else:
                        st.write("No text columns found to trim.")

                # Tab 4: Reshape & Aggregate
                with tabs[3]:
                    st.markdown("##### 🧮 Add Calculated Column (Formula)")
                    st.info("Create a new column using a formula. Use backticks for column names with spaces, e.g., `` `Unit Price` * Quantity ``")

                    c1, c2 = st.columns(2)
                    new_calc_col_name = c1.text_input("New column name", key="new_calc_col")
                    formula = c2.text_input("Formula", placeholder="e.g., Price * 1.2", key="formula_input")

                    if st.button("Create Calculated Column", use_container_width=True, key="calc_col_btn"):
                        if new_calc_col_name and formula:
                            try:
                                # Use pandas.eval for safe evaluation. The expression creates the new column.
                                st.session_state.converter_df.eval(f"`{new_calc_col_name}` = {formula}", inplace=True)
                                st.success(f"Created column '{new_calc_col_name}'.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Formula error: {e}")
                        else:
                            st.warning("Please provide a new column name and a formula.")

                    st.divider()
                    st.markdown("##### 🔄 Pivot & Aggregate Data")
                    st.info("Summarize your data by transforming rows into a summary table (similar to an Excel PivotTable).")

                    numeric_cols = df.select_dtypes(include=np.number).columns.tolist()

                    pivot_index = st.multiselect("Rows (Group By)", options=df.columns, key="pivot_index")
                    pivot_cols = st.multiselect("Columns (Pivot On)", options=df.columns, key="pivot_cols")
                    pivot_values = st.multiselect("Values (Aggregate)", options=numeric_cols, key="pivot_values")
                    agg_func = st.selectbox("Aggregation Function", ["sum", "mean", "count", "max", "min"], key="pivot_agg")

                    if st.button("Apply Pivot", use_container_width=True, key="pivot_btn"):
                        if pivot_index and pivot_values:
                            try:
                                pivoted_df = pd.pivot_table(
                                    st.session_state.converter_df,
                                    index=pivot_index,
                                    columns=pivot_cols if pivot_cols else None,
                                    values=pivot_values,
                                    aggfunc=agg_func
                                ).reset_index()

                                # Clean up column names if they are multi-level after pivot
                                if isinstance(pivoted_df.columns, pd.MultiIndex):
                                    pivoted_df.columns = ['_'.join(map(str, col)).strip().strip('_') for col in pivoted_df.columns.values]

                                st.session_state.converter_df = pivoted_df
                                st.success("Data successfully pivoted.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Pivot failed: {e}")
                        else:
                            st.warning("Please select at least 'Rows (Group By)' and 'Values (Aggregate)' to create a pivot table.")

                # Tab 4: AI Repair Suggestions
                if MLENGINE_AVAILABLE:
                    with tabs[4]:
                        st.markdown("##### AI-Powered Data Quality Suggestions")
                        st.write("Uses fuzzy matching to find and suggest fixes for potential typos in categorical data (e.g., 'NY' vs 'New York').")

                        if "repair_suggestions" not in st.session_state:
                            st.session_state.repair_suggestions = None

                        if st.button("Scan for Repair Suggestions"):
                            with st.spinner("🤖 AI is scanning your data..."):
                                st.session_state.repair_suggestions = MLEngine.suggest_data_repairs(df)
                            st.rerun()

                        if st.session_state.repair_suggestions is not None:
                            suggestions = st.session_state.repair_suggestions
                            if not suggestions:
                                st.success("✅ Scan complete. No obvious typos found!")
                            else:
                                st.warning(f"Found {len(suggestions)} potential issues.")
                                for i, sug in enumerate(suggestions):
                                    col, original, suggested, rows = sug['column'], sug['original_value'], sug['suggested_value'], sug['affected_rows']
                                    st.markdown(f"**Column `{col}`**: Found `{original}` ({rows} rows). Did you mean `{suggested}`?")
                                    if st.button(f"Fix: Change '{original}' to '{suggested}'", key=f"fix_{i}"):
                                        st.session_state.converter_df[col] = st.session_state.converter_df[col].replace(original, suggested)
                                        st.session_state.repair_suggestions.pop(i)
                                        st.rerun()

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