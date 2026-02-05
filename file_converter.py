import streamlit as st
import pandas as pd
import io

def page_file_converter():
    st.title("🔄 Self-Service File Converter")
    st.markdown("""
    **Utility:** Convert data files between common formats instantly without writing code.
    **Supported Formats:** CSV, Excel (XLSX), JSON, Parquet.
    """)

    uploaded_file = st.file_uploader("Upload Source File", type=["csv", "xlsx", "xls", "json", "parquet"])

    if uploaded_file:
        file_ext = uploaded_file.name.split('.')[-1].lower()
        df = None
        
        try:
            # Load Data
            if file_ext == 'csv':
                df = pd.read_csv(uploaded_file)
            elif file_ext in ['xlsx', 'xls']:
                df = pd.read_excel(uploaded_file)
            elif file_ext == 'json':
                df = pd.read_json(uploaded_file)
            elif file_ext == 'parquet':
                df = pd.read_parquet(uploaded_file)
            
            if df is not None:
                st.success(f"✅ Loaded **{uploaded_file.name}** ({df.shape[0]} rows, {df.shape[1]} columns)")
                
                with st.expander("🔎 Preview Data", expanded=True):
                    st.dataframe(df.head())

                st.divider()
                st.subheader("Conversion Options")
                
                col1, col2 = st.columns(2)
                with col1:
                    target_format = st.selectbox("Select Output Format", ["CSV", "Excel", "JSON", "Parquet"])
                
                with col2:
                    st.write("") # Spacer
                    st.write("") # Spacer
                    convert_btn = st.button("Convert & Prepare Download", type="primary")

                if convert_btn:
                    output_buffer = io.BytesIO()
                    mime_type = ""
                    new_filename = uploaded_file.name.rsplit('.', 1)[0]
                    
                    try:
                        if target_format == "CSV":
                            df.to_csv(output_buffer, index=False)
                            mime_type = "text/csv"
                            new_filename += ".csv"
                        elif target_format == "Excel":
                            # Defaults to openpyxl or xlsxwriter depending on environment
                            with pd.ExcelWriter(output_buffer) as writer:
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
                            mime=mime_type
                        )
                    except Exception as e:
                        st.error(f"Conversion failed: {str(e)}")
                        
        except Exception as e:
            st.error(f"Error reading file: {str(e)}")