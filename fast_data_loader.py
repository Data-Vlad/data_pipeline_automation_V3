import logging
import os
import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)

def load_data_high_performance(file_path: str) -> pd.DataFrame:
    """
    Universal high-performance file loader for large datasets (1M+ records).
    
    Industry Best Practice (2026+):
    Uses the Polars engine (Rust-backed) for structured data to achieve 10-50x speedups 
    over legacy Python parsers.
    
    Supported Formats:
    - Excel (.xlsx, .xls, .xlsb): Uses 'calamine' (memory-mapped, zero-copy).
    - CSV/PSV/TXT: Uses Polars multi-threaded CSV reader.
    - Parquet: Uses Polars memory-mapped reader.
    - PDF: Uses pdfplumber (Note: significantly slower, not recommended for 1M+ rows).
    """
    ext = os.path.splitext(file_path)[1].lower()
    
    try:
        import polars as pl
    except ImportError:
        error_msg = "High-performance library 'polars' missing."
        logger.error(error_msg)
        raise ImportError(error_msg)

    logger.info(f"Starting high-performance load of '{os.path.basename(file_path)}' using Polars engine...")

    df_pl = None

    # 1. Excel (Rust-based Calamine engine)
    if ext in ['.xlsx', '.xls', '.xlsb', '.xlsm']:
        try:
            import fastexcel # Required for engine='calamine'
        except ImportError:
            raise ImportError("Library 'fastexcel' is required for high-performance Excel parsing.")
        
        # engine='calamine' is the key to 2026+ performance standards
        df_pl = pl.read_excel(source=file_path, engine="calamine")

    # 2. CSV / Text (Multi-threaded SIMD reader)
    elif ext in ['.csv', '.txt', '.psv']:
        # Polars automatically handles CSV parsing much faster than pandas
        try:
            df_pl = pl.read_csv(file_path, ignore_errors=True, try_parse_dates=True)
        except Exception:
            # Fallback for common delimiter issues
            df_pl = pl.read_csv(file_path, separator='\t', ignore_errors=True, try_parse_dates=True)

    # 3. Parquet (Native format for Arrow/Polars)
    elif ext == '.parquet':
        df_pl = pl.read_parquet(file_path)

    # 4. PDF (Unstructured - Fallback)
    elif ext == '.pdf':
        return _load_pdf_tables(file_path)

    else:
        raise ValueError(f"Unsupported file format for high-performance loader: {ext}")

    row_count = len(df_pl)
    logger.info(f"Loaded {row_count} rows. Converting to Pandas for pipeline compatibility...")
    
    # Convert to Pandas for downstream compatibility
    return df_pl.to_pandas()

def _load_pdf_tables(file_path: str) -> pd.DataFrame:
    """
    Helper to extract tables from PDF.
    Performance Warning: PDF is not designed for large datasets. 
    """
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("Library 'pdfplumber' is required for PDF parsing.")

    logger.warning("Parsing PDF with 1M+ records is inefficient. Recommended: Convert to CSV/Parquet upstream.")
    
    all_rows = []
    with pdfplumber.open(file_path) as pdf:
        total_pages = len(pdf.pages)
        logger.info(f"Processing {total_pages} pages of PDF data...")
        
        for i, page in enumerate(pdf.pages):
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    # Clean newlines which often break data structure
                    cleaned_row = [cell.replace('\n', ' ').strip() if cell else None for cell in row]
                    all_rows.append(cleaned_row)
            
            if (i + 1) % 50 == 0:
                logger.info(f"Processed {i + 1}/{total_pages} pages...")

    if not all_rows:
        return pd.DataFrame()

    # Assume first row is header
    return pd.DataFrame(all_rows[1:], columns=all_rows[0])