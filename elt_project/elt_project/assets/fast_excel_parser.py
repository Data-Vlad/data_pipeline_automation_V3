import pandas as pd
import logging
import os

# Configure logging
logger = logging.getLogger(__name__)

def parse_excel_high_performance(file_path: str) -> pd.DataFrame:
    """
    Parses very large Excel files (1M+ records) using the Polars library backed by the Calamine engine.
    
    Industry Best Practice (2026+):
    For high-volume Excel ingestion, pure Python parsers (like openpyxl) are deprecated in favor of 
    Rust-based parsers. This function uses 'calamine' (via Polars/fastexcel) which reads Excel 
    files as memory-mapped binary streams, providing near-instant parsing speeds and significantly 
    lower memory overhead compared to legacy methods.
    """
    try:
        import polars as pl
        # fastexcel is the python wrapper for the Rust 'calamine' crate, required for engine='calamine'
        import fastexcel 
    except ImportError:
        error_msg = "High-performance libraries missing. Please ensure 'polars' and 'fastexcel' are installed."
        logger.error(error_msg)
        raise ImportError(error_msg)

    logger.info(f"Starting high-performance parse of '{os.path.basename(file_path)}' using Polars/Calamine engine...")
    
    # Read the Excel file into a Polars DataFrame.
    # engine="calamine" is crucial for performance.
    df_pl = pl.read_excel(source=file_path, engine="calamine")
    
    row_count = len(df_pl)
    logger.info(f"Parsed {row_count} rows. Converting to Pandas for pipeline compatibility...")
    
    # Convert to Pandas DataFrame as required by the framework's contract.
    # While Polars is faster, the downstream SQL loader likely expects a Pandas DataFrame.
    return df_pl.to_pandas()