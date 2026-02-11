import logging
import os
import pandas as pd
import sys

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
    - Parquet: Uses Polars memory-mapped reader (Native Arrow).
    - JSON/NDJSON: Uses Polars JSON reader.
    - PDF: Uses pdfplumber (Note: significantly slower, not recommended for 1M+ rows).
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    ext = os.path.splitext(file_path)[1].lower()
    
    try:
        import polars as pl
    except ImportError:
        error_msg = "High-performance library 'polars' missing. Please install via: pip install polars"
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

    # 4. JSON (Structured)
    elif ext in ['.json', '.ndjson', '.jsonl']:
        try:
            # Try Newline Delimited JSON first (common for large data)
            df_pl = pl.read_ndjson(file_path)
        except Exception:
            df_pl = pl.read_json(file_path)

    # 5. PDF (Unstructured - Fallback)
    elif ext == '.pdf':
        return _load_pdf_tables(file_path)

    else:
        raise ValueError(f"Unsupported file format for high-performance loader: '{ext}' (File: {os.path.basename(file_path)}). Supported formats: Excel (.xlsx, .xls), CSV/TXT, Parquet, JSON, PDF.")

    row_count = len(df_pl)
    logger.info(f"Loaded {row_count} rows. Converting to Pandas for pipeline compatibility...")
    
    # Convert to Pandas for downstream compatibility
    # Uses pyarrow for zero-copy conversion if installed
    return df_pl.to_pandas(use_pyarrow_extension_array=True) if row_count > 0 else pd.DataFrame()

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
                if not table: continue
                for row in table:
                    # Clean newlines which often break data structure
                    cleaned_row = [cell.replace('\n', ' ').strip() if cell else None for cell in row]
                    all_rows.append(cleaned_row)
            
            if (i + 1) % 50 == 0:
                logger.info(f"Processed {i + 1}/{total_pages} pages...")

    if not all_rows:
        return pd.DataFrame()

    # Assume first row is header
    headers = all_rows[0]
    # Deduplicate headers to prevent Pandas errors
    seen = {}
    deduped_headers = []
    for h in headers:
        h_str = str(h) if h else "Column"
        if h_str in seen:
            seen[h_str] += 1
            deduped_headers.append(f"{h_str}_{seen[h_str]}")
        else:
            seen[h_str] = 0
            deduped_headers.append(h_str)

    return pd.DataFrame(all_rows[1:], columns=deduped_headers)

if __name__ == "__main__":
    # CLI Entry point for testing the mechanism
    import time
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) < 2:
        print("Usage: python fast_data_loader.py <file_path>")
    else:
        start_time = time.time()
        df = load_data_high_performance(sys.argv[1])
        elapsed = time.time() - start_time
        print(f"Success! Loaded {len(df)} rows in {elapsed:.4f} seconds.")
        print(df.head())