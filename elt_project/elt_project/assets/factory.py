# elt_project/assets/factory.py
import pandas as pd
from datetime import datetime
import re
import os # Ensure os is imported
import json
from typing import Optional, List
import glob # Import glob at top level for reliability
import traceback # Import traceback to capture detailed error info
from sqlalchemy import create_engine
from sqlalchemy import text, inspect
from dagster import asset, AssetExecutionContext, AssetKey
from dagster import MetadataValue, Config
from .models import PipelineConfig # This is correct, models.py is in the same directory
from .resources import SQLServerResource
from . import parsers, custom_parsers
from .sql_loader import load_df_to_sql, execute_stored_procedure, load_csv_to_sql_chunked

def sanitize_name(name: str) -> str:
    """
    Sanitizes a string to be a valid Dagster asset name by replacing
    all non-alphanumeric characters with underscores.
    """
    return re.sub(r'[^A-Za-z0-9_]', '_', name)

def _log_asset_run(engine, log_details: dict):
    """
    Inserts a detailed log entry for an asset run into the database.

    This function connects to the database and inserts a record into the
    `etl_pipeline_run_logs` table. It's designed to be resilient; if the
    database write fails, it prints a critical error to the console but
    does not fail the asset run itself, prioritizing data processing over
    logging.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine to use for the connection.
        log_details (dict): A dictionary containing all log information.
    """
    log_stmt = text("""
        INSERT INTO etl_pipeline_run_logs (
            run_id, pipeline_name, import_name, asset_name, status,
            start_time, end_time, rows_processed, message, error_details, resolution_steps
        ) VALUES (
            :run_id, :pipeline_name, :import_name, :asset_name, :status,
            :start_time, :end_time, :rows_processed, :message, :error_details, :resolution_steps
        )
    """)
    try:
        with engine.connect() as connection:
            with connection.begin() as transaction:
                connection.execute(log_stmt, log_details)
                transaction.commit()
    except Exception as e:
        print(f"CRITICAL: Failed to write to etl_pipeline_run_logs. Error: {e}. Original log details: {log_details}")

def _show_toast_notification(status: str, pipeline_name: str, import_name: str, source_file: str, message: str):
    """Displays a Windows toast notification to provide immediate user feedback.

    Args:
        status (str): 'SUCCESS' or 'FAILURE'.
        pipeline_name (str): The name of the pipeline.
        import_name (str): The name of the specific import process.
        source_file (str): The basename of the file that was processed.
        message (str): A user-friendly message about the outcome.
    """
    # Toast notifications disabled as BurntToast module is not available.
    pass

def _write_user_feedback_log(monitored_directory: Optional[str], pipeline_name: str, import_name: str, status: str, source_file: str, message: str):
    """
    Writes a user-friendly log entry to a file in the monitored directory.

    This creates a file like '2023-11-20__run_history.log' to give users immediate
    feedback on processing status without needing to check the Dagster UI.

    Args:
        import_name (str): The name of the specific import process.
        pipeline_name (str): The name of the pipeline, used to create a log subdirectory.
        monitored_directory (str): The directory where the source file was found.
        status (str): 'SUCCESS' or 'FAILURE'.
        source_file (str): The basename of the file that was processed.
        message (str): A user-friendly message about the outcome.
    """
    if not monitored_directory:
        return
    if not os.path.isdir(monitored_directory):
        return  # Can't write a log if the directory doesn't exist.
    
    # --- Log Rotation: Keep only the current day's log ---
    current_log_basename = f"{datetime.utcnow().strftime('%Y-%m-%d')}__run_history.log"
    try:
        for filename in os.listdir(monitored_directory):
            # Check if the file is a run history log but not for the current day
            if "__run_history.log" in filename and filename != current_log_basename:
                try:
                    os.remove(os.path.join(monitored_directory, filename))
                    print(f"Cleaned up old log file: {filename}")
                except OSError as e:
                    print(f"Warning: Could not remove old log file '{filename}'. Error: {e}")
    except Exception as e:
        print(f"Warning: An error occurred during log file cleanup. Error: {e}")

    log_filename = os.path.join(monitored_directory, current_log_basename)
    log_entry = f"[{datetime.utcnow().isoformat()}] - {status} - File: {os.path.basename(source_file)} - Details: {message}\n"
 
    with open(log_filename, "a", encoding="utf-8") as f:
        f.write(log_entry)

def create_extract_and_load_asset(config: PipelineConfig):
    """Asset factory for creating the 'extract_and_load_staging' asset.

    This factory generates a dynamic Dagster asset responsible for the first two stages
    of the ELT process: Extract and Load. For each `PipelineConfig` provided, it
    constructs an asset that can:
    - Extract data from a source (local file, web scrape via requests or Selenium).
    - Parse the data using either a generic parser from a factory or a whitelisted
      custom parser function.
    - Load the resulting DataFrame into a specified staging table in SQL Server.
    - Execute data quality checks against the newly loaded data.
    - Log detailed metrics, status, and errors of its execution to a dedicated
      SQL logging table (`etl_pipeline_run_logs`).
    - Handle security by whitelisting executable custom functions and sanitizing file paths.

    Args:
        config (PipelineConfig): The configuration object defining all metadata for a
            single data pipeline, such as file patterns, table names, and parser types.

    Returns:
        A callable Dagster asset function (`extract_and_load_staging`).
    """

    def get_dynamic_engine(db_resource: SQLServerResource):
        """
        Returns a SQLAlchemy engine based on the pipeline's configuration.

        If the `PipelineConfig` includes a specific `connection_string`, a new
        SQLAlchemy engine is created using that string. This allows individual
        pipelines to connect to different databases. Otherwise, it returns the
        engine from the default `SQLServerResource` defined for the Dagster job.

        Args:
            db_resource (SQLServerResource): The default SQL Server resource.

        Returns:
            sqlalchemy.engine.Engine: The appropriate engine for the database connection.
        """
        # ALWAYS use the db_resource to get the engine. This ensures that the
        # robust connection string logic in SQLServerResource is always used,
        # preventing DRIVER keyword syntax errors from custom connection strings. This was also correct.
        return db_resource.get_engine()


    # Define a whitelist of allowed custom parser function names.
    # This list MUST be manually updated when new custom parser functions are added to custom_parsers.py.
    ALLOWED_CUSTOM_PARSERS = {
        "parse_ri_dbt_custom",
        "parse_report_with_footer",
        "generic_web_scraper",
        "generic_selenium_scraper",
        "generic_sftp_downloader",
        "generic_configurable_parser",
        # Add other custom parser function names here as they are created, e.g., "parse_my_unique_data_file",
    }
    
    # Sanitize the asset name to be valid in Dagster. This was the source of the error.
    # The old name format was "1. Extract & Stage: {config.import_name}" which is invalid.
    # We now use a sanitized, valid name.
    asset_name = sanitize_name(f"{config.import_name}_extract_and_load_staging")

    # --- NEW: Support for explicit dependencies via scraper_config ---
    deps = []

    @asset(
        name=asset_name, # Use the sanitized name
        group_name=config.pipeline_name.strip().lower(), # Ensure no leading/trailing whitespace before lowercasing
        deps=deps,
        compute_kind="python",
        description=f"""
**Extracts, validates, and stages data for the '{config.import_name}' import.**

This is the first step in the '{config.pipeline_name}' pipeline. It performs the following actions:

1.  **Extracts** data from the source (e.g., file `{config.file_pattern}` or a web scraper).
2.  **Parses** the data into a structured format.
3.  **Loads** the data into the `{config.staging_table}` staging table.
4.  **Validates** the staged data against predefined data quality rules.

If this step succeeds, the data is ready for the final transformation step.
If it fails, check the run logs for details on data quality issues or parsing errors.
""",
    )
    # The resource is passed as an argument to the asset function
    # The source_file_path parameter will be populated by the sensor's run_config
    def extract_and_load_staging(context: AssetExecutionContext, db_resource: SQLServerResource) -> pd.DataFrame:
        """Extracts, parses, validates, and loads data into a staging table.

        This asset is the primary workhorse for the "Extract" and "Load" stages of the
        ELT framework. Its behavior is determined by the `PipelineConfig` used to
        create it.

        The process is as follows:
        1. Determines the source data location (file path from a sensor or a configured path).
        2. Selects a parser: a whitelisted custom function or a generic parser (e.g., for CSV).
        3. Parses the source data into a pandas DataFrame. For web scrapers, it executes the
           configured scraping logic.
        4. For large CSVs, it uses a memory-efficient chunked loading process.
        5. Applies any column renaming specified in the configuration.
        6. Appends a `dagster_run_id` to each row for lineage and incremental processing.
        7. Loads the DataFrame into the specified staging table.
        8. Executes the `sp_execute_data_quality_checks` stored procedure to validate the
           newly loaded data against rules defined in the `data_quality_rules` table.
        9. If a rule with 'FAIL' severity fails, the asset run is halted.
        10. Logs the outcome (success/failure, rows processed, errors) to the database.
        11. Returns the DataFrame for potential downstream use or inspection in the UI.

        Args:
            context (AssetExecutionContext): The Dagster execution context, providing access
                to the run ID, logger, and operational configuration.
            db_resource (SQLServerResource): The configured SQL Server resource for database
                connections.

        Returns:
            pd.DataFrame: The parsed data as a pandas DataFrame. For chunked loads, this
                may be an empty DataFrame as the data is streamed directly to the database.
        """
        engine = get_dynamic_engine(db_resource)
        source_file_path = context.op_config.get("source_file_path")
        resolved_path_for_feedback = source_file_path # Initialize for feedback logging
        csv_path = None # Initialize for cleanup
        file_to_parse = "N/A" # Initialize to prevent UnboundLocalError in finally/except blocks
        
        # --- Runtime Config Fetch ---
        # Fetch the latest staging table name to handle config updates without full restart
        current_staging_table = config.staging_table
        try:
            with engine.connect() as connection:
                row = connection.execute(
                    text("SELECT staging_table FROM elt_pipeline_configs WHERE import_name = :import_name"),
                    {"import_name": config.import_name}
                ).mappings().one_or_none()
                if row and row['staging_table']:
                    current_staging_table = row['staging_table']
                    if current_staging_table != config.staging_table:
                        context.log.info(f"Runtime config override: Using staging table '{current_staging_table}' (Configured: '{config.staging_table}')")
        except Exception as e:
            context.log.warning(f"Failed to fetch runtime config for staging table: {e}. Using cached value.")

        start_time = datetime.utcnow()

        log_details = {
            "run_id": context.run_id,
            "pipeline_name": config.pipeline_name,
            "import_name": config.import_name,
            # Use the passed source_file_path for logging if available, otherwise fall back to config.file_pattern
            "source_file_path_used": source_file_path if source_file_path else config.file_pattern, 
            "asset_name": context.asset_key.to_user_string(), 
            "start_time": start_time, 
            "end_time": None, "status": "FAILURE", "rows_processed": 0, "message": "",
            "error_details": None, "resolution_steps": None # Initialize to None
        }

        try:
            context.log.info(f"Starting extraction for {config.import_name}...")
            
            # --- NOTIFY STARTED (Only if Root Pipeline) ---
            # We only want to notify "Started" for the first import in a chain.
            if not config.depends_on:
                _show_toast_notification(
                    status="STARTED",
                    pipeline_name=config.pipeline_name,
                    import_name=config.import_name,
                    source_file=source_file_path if source_file_path else (config.file_pattern or "Manual Run"),
                    message="Pipeline execution started."
                )
                # We don't necessarily need a persistent log file for "Started", 
                # but it can be useful for debugging.
                # _write_user_feedback_log(..., "STARTED", ...) 


            # Determine the actual file path to parse
            if source_file_path:
                file_to_parse = source_file_path
                
                # --- WORKAROUND: Handle Excel Lock Files ---
                # If the sensor triggers on a lock file (~$File.xlsx), redirect to the real file (File.xlsx).
                # This ensures data is loaded even if the sensor picks up the lock file instead of the data file.
                if os.path.basename(file_to_parse).startswith("~$"):
                    real_filename = os.path.basename(file_to_parse)[2:] # Remove the ~$ prefix
                    real_file_path = os.path.join(os.path.dirname(file_to_parse), real_filename)
                    if os.path.isfile(real_file_path):
                        context.log.info(f"Redirecting from lock file '{os.path.basename(file_to_parse)}' to real file '{real_filename}'")
                        file_to_parse = real_file_path
                    else:
                        context.log.warning(f"Triggered on lock file '{file_to_parse}' but real file not found. Skipping.")
                        log_details["status"] = "SKIPPED"
                        log_details["message"] = "Ignored orphan Excel lock file."
                        return pd.DataFrame()
            else:
                # If source_file_path is not provided (e.g., manual run), construct the full path.
                # SECURITY: Sanitize the file_pattern to prevent path traversal attacks.
                # We only want the filename, not any directory info that might be in the pattern.
                pattern_to_use = config.file_pattern
                if pattern_to_use:
                    pattern_to_use = os.path.basename(pattern_to_use)
                file_to_parse = os.path.join(config.monitored_directory, pattern_to_use) if config.monitored_directory else pattern_to_use
            if not file_to_parse:
                raise ValueError("No source file path provided or configured for extraction.")

            # --- Resolve wildcards/globs for ALL file types ---
            # This ensures that Excel, PSV, and custom parsers also support file patterns.
            # Check if it is an existing file first. This prevents glob from breaking on paths with brackets [].
            if not os.path.isfile(file_to_parse) and any(ch in str(file_to_parse) for ch in ["*", "?", "["]):
                matches = glob.glob(file_to_parse, recursive=True)
                if not matches and config.monitored_directory:
                    pattern2 = os.path.join(config.monitored_directory, os.path.basename(file_to_parse))
                    matches = glob.glob(pattern2, recursive=True)

                # Filter out Excel temporary lock files (starting with ~$) which match *.xlsx but are not readable
                matches = [m for m in matches if not os.path.basename(m).startswith("~$")]

                if not matches:
                    context.log.warning(f"Source file not found for {config.import_name}: '{file_to_parse}'. Skipping.")
                    log_details["status"] = "SUCCESS"
                    log_details["message"] = f"Source file not found: '{file_to_parse}'. No data loaded."
                    return pd.DataFrame()
                
                resolved_path = max(matches, key=os.path.getmtime)
                context.log.info(f"Resolved pattern '{file_to_parse}' to file '{resolved_path}'")
                file_to_parse = resolved_path
            
            resolved_path_for_feedback = file_to_parse

            # --- NEW: Determine processing type (allows for overrides like Excel->CSV conversion) ---
            processing_file_type = config.file_type.strip().lower()

            # --- WORKAROUND: Auto-convert Excel to CSV for memory efficiency ---
            # Check config OR file extension to catch all Excel files.
            is_excel_config = processing_file_type in ['excel', 'xlsx', 'xls']
            is_excel_file = file_to_parse.lower().endswith(('.xlsx', '.xls'))

            if (is_excel_config or is_excel_file) and not config.parser_function:
                try:
                    context.log.info(f"Auto-converting Excel file to CSV for memory-efficient loading: {file_to_parse}")
                    # Generate CSV path
                    csv_path = os.path.splitext(file_to_parse)[0] + ".converted.csv"
                    
                    # Read Excel
                    # Explicitly specify engine for .xlsx to avoid "format cannot be determined" errors
                    engine = 'openpyxl' if file_to_parse.lower().endswith('.xlsx') else None
                    df_temp = pd.read_excel(file_to_parse, engine=engine)
                    
                    # Save to CSV (using latin1 to match sql_loader default, errors='replace' to prevent crash)
                    df_temp.to_csv(csv_path, index=False, encoding='latin1', errors='replace')
                    
                    # Cleanup memory
                    del df_temp
                    import gc
                    gc.collect()
                    
                    context.log.info(f"Conversion successful. Switching processing mode to CSV using file: {csv_path}")
                    file_to_parse = csv_path
                    processing_file_type = 'csv'
                    
                except Exception as e:
                    # If the error is missing dependency, fail immediately instead of falling back
                    if "openpyxl" in str(e) and "dependency" in str(e):
                        raise e
                    context.log.warning(f"Excel-to-CSV conversion failed: {e}. Falling back to standard Excel parsing.")

            # OPTIMIZATION: Use chunked loading for standard CSVs to save memory
            if processing_file_type == 'csv' and not config.parser_function:
                try:
                    context.log.info(f"Using memory-efficient chunked CSV loader for {file_to_parse}")
                    rows_processed = load_csv_to_sql_chunked(
                        file_path=file_to_parse,
                        table_name=current_staging_table,
                        engine=engine,
                        run_id=context.run_id,
                        column_mapping=config.get_column_mapping()
                    )
                    log_details["rows_processed"] = rows_processed
                    context.log.info(f"Successfully loaded {rows_processed} rows in chunks.")
                    context.add_output_metadata({"num_rows": rows_processed, "staging_table": current_staging_table})
                except FileNotFoundError:
                    context.log.warning(f"Source file not found for {config.import_name}: '{file_to_parse}'. Skipping.")
                    # No user feedback log here, as no file was found to be "processed". The sensor just moves on.
                    log_details["status"] = "SUCCESS"
                    log_details["message"] = f"Source file not found: '{file_to_parse}'. No data loaded."
                    return pd.DataFrame()

                # Return an empty DataFrame as we didn't load the whole thing into memory
                df = pd.DataFrame()
            else:
                # --- Fallback to original in-memory parsing for other file types or custom parsers ---
                try:
                    df: pd.DataFrame
                    if config.parser_function:
                        # For custom parsers, the file_to_parse is what we have.
                        if config.parser_function not in ALLOWED_CUSTOM_PARSERS:
                            raise ValueError(f"Custom parser function '{config.parser_function}' is not whitelisted.")
                        context.log.info(f"Using custom parser function '{config.parser_function}' for file: {file_to_parse}")
                        custom_parser_func = getattr(custom_parsers, config.parser_function)
                        # SECURITY: Add an extra check to ensure the retrieved attribute is actually a function.
                        if not callable(custom_parser_func):
                            raise TypeError(f"The specified parser '{config.parser_function}' is not a callable function.")

                        # Differentiate between file-based parsers and config-based scrapers
                        if config.parser_function in ["generic_web_scraper", "generic_selenium_scraper"]:
                            if not config.scraper_config:
                                raise ValueError(f"'{config.parser_function}' requires a non-null 'scraper_config' in the database.")
                            # Scrapers can return one DataFrame or a dict of them
                            scraped_result = custom_parser_func(config.scraper_config)
                            if isinstance(scraped_result, dict):
                                # If it's a dict, find the DataFrame for the current asset's import_name
                                df = scraped_result.get(config.import_name)
                                if df is None:
                                    raise ValueError(f"Scraper did not return a DataFrame for target_import_name '{config.import_name}'. Available targets: {list(scraped_result.keys())}")
                            else:
                                df = scraped_result # It's a single DataFrame                        
                        elif config.parser_function == "generic_configurable_parser":
                            if not config.scraper_config: # We'll reuse the scraper_config column
                                raise ValueError(f"'generic_configurable_parser' requires a non-null 'scraper_config' in the database to hold the parser JSON.")
                            # This parser needs both the file path and the config
                            df = custom_parser_func(file_to_parse, config.scraper_config)
                        else:
                            df = custom_parser_func(file_to_parse)
                    else:
                        # For factory parsers, the file_to_parse is what we have.
                        parser = parsers.parser_factory.get_parser(config.file_type)
                        context.log.info(f"Using '{config.file_type}' parser from factory for file: {file_to_parse}")
                        df = parser.parse(file_to_parse)
                except FileNotFoundError:
                    context.log.warning(f"Source file not found for {config.import_name}: '{file_to_parse}'. Skipping.")
                    # No user feedback log here, as no file was found to be "processed".
                    log_details["status"] = "SUCCESS"
                    log_details["message"] = f"Source file not found: '{file_to_parse}'. No data loaded."
                    return pd.DataFrame()

                df["dagster_run_id"] = context.run_id

                # --- Column Mapping Logic ---
                # For in-memory loads, we only apply explicit mappings. The chunked loader handles auto-mapping.
                if config.get_column_mapping():
                    df.rename(columns=config.get_column_mapping(), inplace=True)

                bool_cols = [col for col in df.columns if 'checkbox' in col.lower() or 'canbecompleted' in col.lower()]
                if bool_cols:
                    context.log.info(f"Filling NaN values with 0 for boolean columns: {bool_cols}")
                    df[bool_cols] = df[bool_cols].fillna(0)

                rows_processed = len(df)
                log_details["rows_processed"] = rows_processed
                context.log.info(f"Successfully parsed {rows_processed} rows.")
                
                context.log.info(f"Loading data into staging table: {current_staging_table}")
                load_df_to_sql(df, current_staging_table, engine)

                # --- DATA GOVERNANCE: Execute Data Quality Checks ---
                context.log.info(f"Executing data quality checks for table: {current_staging_table}")
                with engine.connect() as connection: # This was already correct, no change needed here.
                    # We use a direct execution here to get the output value (total failing rows)
                    result = connection.execute(
                        text("EXEC sp_execute_data_quality_checks @run_id=:run_id, @target_table=:target_table"),
                        {"run_id": context.run_id, "target_table": current_staging_table}
                    ).scalar_one_or_none()
                    total_failing_rows = result if result is not None else 0
                    context.log.info(f"Data quality checks completed. Total failing rows: {total_failing_rows}")

                    # Check if any 'FAIL' severity rules failed
                    fail_rules_failed_count = connection.execute(
                        text("""
                            SELECT COUNT(*) FROM data_quality_run_logs l
                            JOIN data_quality_rules r ON l.rule_id = r.rule_id
                            WHERE l.run_id = :run_id AND r.target_table = :target_table AND l.status = 'FAIL' AND r.severity = 'FAIL'
                        """),
                        {"run_id": context.run_id, "target_table": current_staging_table}
                    ).scalar()
                    if fail_rules_failed_count > 0:
                        raise Exception(f"{fail_rules_failed_count} critical data quality rule(s) failed. Halting pipeline run. Check 'data_quality_run_logs' for details.")

                context.log.info("Load to staging complete.")
                
                context.add_output_metadata({
                    "num_rows": rows_processed, "staging_table": current_staging_table,
                    "preview": df.head().to_markdown(),
                })

            log_details["status"] = "SUCCESS"
            log_details["message"] = f"Successfully processed and loaded {log_details['rows_processed']} rows into {current_staging_table}."

        except Exception as e:
            # --- Smart Error Handling ---
            error_msg = str(e)

            # Check for missing dependencies (specifically openpyxl for Excel)
            if "openpyxl" in error_msg and "dependency" in error_msg:
                log_details["resolution_steps"] = "The 'openpyxl' library is required to process Excel files. Please install it by running: pip install openpyxl"
                log_details["error_details"] = error_msg
            # Check for Column Mismatches
            elif "Invalid column name" in error_msg or ("ProgrammingError" in str(type(e)) and "42S22" in error_msg):
                try:
                    # Introspect the database to get the actual table columns
                    from sqlalchemy import inspect
                    inspector = inspect(engine)
                    table_columns = [col['name'] for col in inspector.get_columns(current_staging_table)]
                    
                    # Craft a helpful, actionable error message
                    resolution_steps = (
                        f"Database schema mismatch. The columns in your data do not match the columns in the staging table '{current_staging_table}'.\n"
                        f"  > Columns in your data (after mapping): {list(df.columns) if 'df' in locals() else 'Could not determine'}\n"
                        f"  > Columns in SQL table '{current_staging_table}': {table_columns}\n"
                        f"  > ACTION: Update the 'column_mapping' in 'elt_pipeline_configs' for import_name '{config.import_name}' to map your source columns to the SQL table columns."
                    )
                    log_details["resolution_steps"] = resolution_steps
                except Exception as inspect_e:
                    # If introspection fails, fall back to a generic message
                    log_details["resolution_steps"] = f"A database schema error occurred. Could not automatically inspect table columns due to: {inspect_e}. Please manually check that the columns in your source file (or your column_mapping) match the schema of the table '{current_staging_table}'."
            else:
                # SECURITY: Avoid logging full stack traces to the database to prevent information disclosure.
                # The full trace is still available in the Dagster UI/console for developers.
                log_details["error_details"] = f"An unexpected error of type {type(e).__name__} occurred."
                log_details["resolution_steps"] = f"Review Dagster logs and stack trace for '{config.import_name}_extract_and_load_staging'. Check source file format, path ('{file_to_parse}'), and custom parser logic if applicable. Ensure staging table schema matches parsed data."
            log_details["message"] = str(e)
            context.log.error(f"Error during extraction for {config.import_name}: {e}")
            
            # --- NOTIFY ON FAILURE ONLY ---
            # We only notify here if extraction fails. Success notifications are deferred to the Transform asset.
            _show_toast_notification(
                status="FAILURE",
                pipeline_name=config.pipeline_name,
                import_name=config.import_name,
                source_file=resolved_path_for_feedback or "N/A",
                message=log_details["message"]
            )
            _write_user_feedback_log(
                monitored_directory=config.monitored_directory,
                pipeline_name=config.pipeline_name,
                import_name=config.import_name,
                status="FAILURE",
                source_file=resolved_path_for_feedback or "N/A",
                message=log_details["message"]
            )

            # The original exception must be re-raised here to fail the asset correctly.
            raise  # Re-raise the exception to fail the Dagster asset

        finally:
            log_details["end_time"] = datetime.utcnow()
            # Write to the database log for long-term storage.
            _log_asset_run(engine, log_details)

            # Cleanup temporary converted CSV if it exists
            if csv_path and os.path.exists(csv_path):
                try:
                    os.remove(csv_path)
                    context.log.info(f"Cleaned up temporary CSV file: {csv_path}")
                except Exception as e:
                    context.log.warning(f"Failed to delete temporary CSV file '{csv_path}': {e}")
        return df

    return extract_and_load_staging

def create_transform_asset(config: PipelineConfig):
    """Asset factory for creating the 'transform' asset for a single import.

    This factory generates a transform asset for a specific import. This asset depends
    on the corresponding `_extract_and_load_staging` asset. Its primary responsibility
    is to orchestrate the "Transform" step of the ELT process for this specific import,
    which is executed by a SQL stored procedure.

    The generated asset will:
    1. Wait for the upstream staging asset to complete successfully.
    2. Determine if the destination table should be truncated based on the
       run's trigger (sensor vs. manual) and the `load_method` of the configs.
    3. Execute the configured SQL stored procedure, passing the `dagster_run_id` and
       the list of tables to truncate.
    4. After the transformation, it cleans up the staging tables by deleting all
       rows associated with the current `dagster_run_id`.
    5. Log the outcome of the transformation to the `etl_pipeline_run_logs` table.

    Args:
        config (PipelineConfig): The configuration for the specific import.

    Returns:
        A callable Dagster asset function (`transform_asset`).
    """
    pipeline_name = config.pipeline_name
    import_name = config.import_name
    transform_procedure = config.transform_procedure
    pipeline_group_name = pipeline_name.strip().lower()

    # Handle multiple destination tables (comma-separated).
    # The first one is treated as the "primary" for locking, smart replace checks, and deduplication.
    all_dest_tables = [t.strip() for t in config.destination_table.split(',')]
    primary_dest_table = all_dest_tables[0]

    # Sanitize the asset name to be valid in Dagster.
    sanitized_transform_name = sanitize_name(f"{import_name}_transform")

    # Define dependencies on the specific extract asset
    deps = [AssetKey(sanitize_name(f"{import_name}_extract_and_load_staging"))]

    # --- NEW: Support for explicit dependencies via scraper_config ---
    # This allows users to chain imports (e.g., ensure 'replace' runs before 'append')
    static_has_upstream_dependency = False
    upstream_imports = []

    # 1. Check explicit 'depends_on' field (Preferred)
    if config.depends_on:
        upstream_imports.extend([d.strip() for d in config.depends_on.split(',') if d.strip()])

    # 2. Check scraper_config (Legacy/Scraper support)
    if config.scraper_config:
        try:
            sc_config = json.loads(config.scraper_config)
            if isinstance(sc_config, dict) and "depends_on" in sc_config:
                # Handle both string and list for depends_on
                depends_on_val = sc_config["depends_on"]
                if isinstance(depends_on_val, str):
                    upstream_imports.append(depends_on_val)
                elif isinstance(depends_on_val, list):
                    upstream_imports.extend(depends_on_val)
        except Exception as e:
            print(f"Warning: Failed to parse scraper_config for '{config.import_name}' during asset creation: {e}")
            pass # Ignore parsing errors

    # Apply dependencies
    for upstream_import in set(upstream_imports):
        # We depend on the upstream's TRANSFORM asset to ensure it is fully complete
        deps.append(AssetKey(sanitize_name(f"{upstream_import}_transform")))
        static_has_upstream_dependency = True

    @asset(
        name=sanitized_transform_name, # Use the sanitized name
        group_name=pipeline_group_name,
        deps=deps,
        description=f"""
**Transforms staged data for '{import_name}' and loads it into the final destination table.**

This is the final, automated step for this import. It waits for the 'Extract & Stage' step to complete successfully, then:

1.  **Executes** the SQL stored procedure `{transform_procedure}`.
2.  **Handles** data replacement or appending based on the configuration.
3.  **Cleans up** the staging table after the run.

This asset moves data from staging to the final, production-ready table.
""",
        compute_kind="sql",
    )
    def transform_asset(context: AssetExecutionContext, db_resource: SQLServerResource):
        """Executes a SQL stored procedure to transform and load data to its final destination.

        This asset orchestrates the final "Transform" and "Load" (to destination) steps.
        Its logic is designed to support both incremental appends and full replacements
        in an atomic and idempotent way.

        The process is as follows:
        1. Determines if the run was triggered by a sensor for a single file or manually
           for the whole group.
        2. Based on the trigger and the `load_method` in the configurations, it builds a
           comma-separated string of destination tables that need to be truncated.
        3. It calls the configured SQL stored procedure, passing both the `dagster_run_id`
           and the list of tables to truncate. The stored procedure is responsible for
           handling the truncation and then inserting the new data from staging.
        4. After the stored procedure succeeds, it deletes the processed data from the
           staging tables for the current run ID to ensure idempotency.
        5. Logs the outcome to the `etl_pipeline_run_logs` table.

        Args:
            context (AssetExecutionContext): The Dagster execution context.
            db_resource (SQLServerResource): The SQL Server resource for database connections.
        """
        # Since this transform runs for the whole group, we can use the connection from the first config
        # or the default resource.
        engine = db_resource.get_engine()
        
        # --- SERIALIZATION LOCK ---
        # Acquire an exclusive application lock on the destination table.
        # This prevents parallel runs (e.g. Replace vs Append) from overwriting each other.
        # We use a dedicated connection for the lock that stays open for the duration of the asset.
        lock_conn = engine.connect()
        lock_resource = f"lock_{primary_dest_table.lower()}"
        try:
            context.log.info(f"Acquiring serialization lock for table '{config.destination_table}'...")
            context.log.info(f"Acquiring serialization lock for table '{primary_dest_table}'...")
            # LockTimeout = -1 means wait indefinitely until the lock is available.
            lock_stmt = text("DECLARE @res INT; EXEC @res = sp_getapplock @Resource = :res, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = -1; SELECT @res;")
            lock_result = lock_conn.execute(lock_stmt, {"res": lock_resource}).scalar()
            
            if lock_result < 0:
                raise Exception(f"Failed to acquire serialization lock for '{lock_resource}'. Result code: {lock_result}")
            
            context.log.info("Lock acquired. Proceeding with transform logic.")

            # --- FETCH LATEST CONFIG ---
            # The config object captured in the closure might be stale (e.g. if load_method changed from replace to append)
            # We fetch the current load_method from the DB to ensure we respect the latest state.
            current_load_method = config.load_method
            current_is_active = config.is_active
            current_staging_table = config.staging_table
            runtime_has_upstream_dependency = False

            try:
                with engine.connect() as connection:
                    # Try fetching with depends_on first, fall back if column doesn't exist yet
                    try:
                        result = connection.execute(
                            text("SELECT load_method, is_active, scraper_config, staging_table, depends_on FROM elt_pipeline_configs WHERE import_name = :import_name"),
                            {"import_name": import_name}
                        ).mappings().one_or_none()
                    except Exception:
                        context.log.warning(f"Column 'depends_on' missing in elt_pipeline_configs. Dependency checks disabled.")
                        result = connection.execute(
                            text("SELECT load_method, is_active, scraper_config, staging_table FROM elt_pipeline_configs WHERE import_name = :import_name"),
                            {"import_name": import_name}
                        ).mappings().one_or_none()

                    if result:
                        current_load_method = result['load_method'].strip() if result['load_method'] else 'append'
                        current_is_active = bool(result['is_active'])
                        if result['staging_table']:
                            current_staging_table = result['staging_table']
                        context.log.info(f"Fetched runtime config for '{import_name}': load_method='{current_load_method}', is_active={current_is_active}")
                        
                        # Check depends_on from DB
                        if 'depends_on' in result and result['depends_on']:
                            runtime_has_upstream_dependency = True
                            context.log.info(f"Runtime check detected dependency on '{result['depends_on']}'. Enforcing append mode.")

                        # Check for dependency at runtime to handle cases where DB was updated but Dagster wasn't reloaded
                        if result['scraper_config']:
                            try:
                                sc_rt = json.loads(result['scraper_config'])
                                if isinstance(sc_rt, dict) and "depends_on" in sc_rt:
                                    runtime_has_upstream_dependency = True
                                    context.log.info(f"Runtime check detected dependency on '{sc_rt['depends_on']}'. Enforcing append mode.")
                            except Exception as e:
                                context.log.error(f"Failed to parse runtime scraper_config for dependency check: {e}. Defaulting to APPEND mode for safety.")
                                runtime_has_upstream_dependency = True # SAFETY: Assume dependency exists to prevent data loss
                    else:
                        context.log.error(f"Runtime config for '{import_name}' not found in database. Defaulting to APPEND mode for safety.")
                        current_load_method = 'append'
            except Exception as e:
                context.log.error(f"Failed to fetch runtime config for '{import_name}': {e}. Defaulting to APPEND mode for safety.")
                current_load_method = 'append'

            start_time = datetime.utcnow()

            # The log entry is for this specific import's transform step
            log_details = {
                "run_id": context.run_id, "pipeline_name": pipeline_name,
                "import_name": import_name, "asset_name": context.asset_key.to_user_string(),
                "start_time": start_time, "end_time": None, "status": "FAILURE", "rows_processed": None, "message": "",
                "error_details": None, "resolution_steps": None # Initialize to None
            }

            # --- DECISION LOGIC: Determine Truncation Strategy ---
            should_truncate = False
            decision_reason = ""

            # Priority 1: Dependencies (Static or Runtime) -> ALWAYS APPEND
            if static_has_upstream_dependency or runtime_has_upstream_dependency:
                should_truncate = False
                decision_reason = "Upstream dependency detected (Static or Runtime)."
            
            # Priority 2: Inactive Pipeline (Queued Run)
            elif not current_is_active:
                if config.on_success_deactivate_self_and_activate_import:
                    should_truncate = False
                    decision_reason = "Pipeline is INACTIVE but part of an auto-switch chain (treating as queued run)."
                else:
                    # Truly inactive, skip execution
                    context.log.warning(f"Pipeline '{import_name}' is INACTIVE in the database. Skipping transform.")
                    log_details["status"] = "SKIPPED"
                    log_details["message"] = "Skipped because pipeline is inactive."
                    log_details["end_time"] = datetime.utcnow()
                    _log_asset_run(engine, log_details)
                    return

            # Priority 3: Configured Load Method
            elif current_load_method.lower() == 'append':
                should_truncate = False
                decision_reason = "Configured as APPEND."
            
            elif current_load_method.lower() == 'replace':
                should_truncate = True
                decision_reason = "Configured as REPLACE."

                # --- SMART REPLACE (BATCH DETECTION) ---
                # If multiple files trigger a REPLACE pipeline simultaneously (e.g. a batch drop),
                # we want the first one to Truncate, and the subsequent ones to Append.
                try:
                    # Check if the destination table was updated very recently (last 2 minutes)
                    # FIX: Use a FRESH connection and WITH (READCOMMITTEDLOCK).
                    # The table hint forces SQL Server to ignore Snapshot Isolation (RCSI) and acquire 
                    # a shared lock to read the absolute latest committed data.
                    with engine.connect() as check_conn:
                        check_time_stmt = text(f"SELECT MAX(load_timestamp), GETUTCDATE() FROM {primary_dest_table} WITH (READCOMMITTEDLOCK)")
                        time_check_row = check_conn.execute(check_time_stmt).fetchone()
                    
                    if time_check_row and time_check_row[0]:
                        last_load_time = time_check_row[0]
                        db_now = time_check_row[1]

                        time_diff = db_now - last_load_time
                        seconds_ago = time_diff.total_seconds()
                        
                        # If updated < 120 seconds ago, assume it's part of the same batch
                        # Allow small negative buffer (-5) for micro-timing differences
                        if -5 <= seconds_ago < 120: 
                            should_truncate = False
                            decision_reason = f"Downgraded REPLACE to APPEND. Table updated {int(seconds_ago)}s ago (Batch detection)."
                            context.log.info(f"Batch detection: Last load was {last_load_time} vs DB Time {db_now} ({int(seconds_ago)}s ago). Switching to APPEND.")
                except Exception as e:
                    context.log.debug(f"Smart Replace check skipped (Table might not have load_timestamp): {e}")
                    context.log.warning(f"Smart Replace skipped. Destination table '{config.destination_table}' might be missing 'load_timestamp' column. Defaulting to TRUNCATE. Error: {e}")
                    context.log.warning(f"Smart Replace skipped. Destination table '{primary_dest_table}' might be missing 'load_timestamp' column. Defaulting to TRUNCATE. Error: {e}")
                    if "Invalid column name" in str(e) or "Invalid object name" in str(e):
                        context.log.warning(f"Smart Replace SKIPPED: Destination table '{config.destination_table}' is missing 'load_timestamp' column or table does not exist. Defaulting to TRUNCATE.")
                        context.log.warning(f"Smart Replace SKIPPED: Destination table '{primary_dest_table}' is missing 'load_timestamp' column or table does not exist. Defaulting to TRUNCATE.")
                    else:
                        context.log.debug(f"Smart Replace check skipped (Table might not have load_timestamp): {e}")
                        context.log.warning(f"Smart Replace skipped. Error: {e}")
            
            else:
                # Fallback for unknown states
                should_truncate = False
                decision_reason = f"Unknown load method '{current_load_method}', defaulting to APPEND."

            try:
                context.log.info(f"Executing transform for '{import_name}': procedure {transform_procedure}")
                context.log.info(f"Decision: Truncate? {should_truncate}. Reason: {decision_reason}")

                tables_to_truncate_str = config.destination_table if should_truncate else None
                context.log.info(f"Tables to truncate: {tables_to_truncate_str or 'None'}")

                # --- Automated Deduplication for 'append' method ---
                # Before calling the transform, if this is an append operation with a deduplication key,
                # we'll clean the staging table to remove rows that already exist in the destination.
                with engine.connect() as connection: # This was also correct.
                    with connection.begin() as transaction:
                        # This logic only applies to 'append' mode with a specified deduplication key.
                        if not should_truncate and config.deduplication_key:
                            context.log.info(f"Performing pre-transform deduplication for '{config.import_name}' on key(s): '{config.deduplication_key}'")
                            
                            # Build the JOIN condition for the DELETE statement
                            key_columns = [key.strip() for key in config.deduplication_key.split(',')]
                            join_conditions = " AND ".join([f"s.[{col}] = d.[{col}]" for col in key_columns])

                            # This SQL statement deletes rows from the staging table (aliased as 's')
                            # where a matching record (based on the deduplication key) already exists
                            # in the destination table (aliased as 'd').
                            # It only considers rows from the current run.
                            dedupe_sql = text(f"""
                                DELETE s
                                FROM {current_staging_table} s
                                JOIN {config.destination_table} d ON {join_conditions}
                                JOIN {primary_dest_table} d ON {join_conditions}
                                WHERE s.dagster_run_id = :run_id
                            """)
                            
                            result = connection.execute(dedupe_sql, {"run_id": context.run_id})
                            
                            if result.rowcount > 0:
                                context.log.info(f"Removed {result.rowcount} duplicate rows from '{current_staging_table}' before transformation.")
                            else:
                                context.log.info(f"No duplicate rows found for '{config.import_name}'.")
                        
                        transaction.commit() # This was also correct.

                # The stored procedure is now expected to handle:
                # 1. Processing only new data identified by the run_id.
                # 2. Conditionally truncating destination tables based on the tables_to_truncate string.
                # This moves the truncation logic into the SQL transaction for better atomicity.
                # The `execute_stored_procedure` function needs to be updated to accept this new parameter.

                row_count = execute_stored_procedure(
                    procedure_name=transform_procedure,
                    engine=engine,
                    run_id=context.run_id,
                    tables_to_truncate=tables_to_truncate_str
                )
                log_details["rows_processed"] = row_count
                context.log.info(f"Transform complete for {pipeline_name}. Rows affected: {row_count}")

                # Generic cleanup: Delete processed data from all staging tables for this run.
                # This prevents re-processing if the transform asset is re-run, ensuring idempotency.
                with engine.connect() as connection: # This was also correct.
                    with connection.begin() as transaction:
                        from sqlalchemy.sql import quoted_name
                        context.log.info(f"Cleaning up staging tables for run_id: {context.run_id}")

                        safe_table_name = quoted_name(current_staging_table, quote=True)
                        delete_stmt = text(f"DELETE FROM {safe_table_name} WHERE dagster_run_id = :run_id")
                        connection.execute(delete_stmt, {"run_id": context.run_id})
                        context.log.info(f"Cleaned up staging table: {current_staging_table}")
                        transaction.commit()

                log_details["status"] = "SUCCESS"

                # --- AUTOMATION: Switch from 'replace' to 'append' mode ---
                # If this was a successful 'replace' run for a config that has the auto-switch enabled.
                if current_load_method.lower() == 'replace' and config.on_success_deactivate_self_and_activate_import:
                    triggering_import_name, target_import_to_activate = config.import_name, config.on_success_deactivate_self_and_activate_import
                    context.log.info(f"Successfully completed 'replace' for '{triggering_import_name}'. Now attempting to deactivate it and activate '{target_import_to_activate}'.")

                    with engine.connect() as connection: # This was also correct.
                        with connection.begin() as transaction:
                            # Deactivate self
                            deactivate_stmt = text("UPDATE elt_pipeline_configs SET is_active = 0 WHERE import_name = :self_import")
                            connection.execute(deactivate_stmt, {"self_import": triggering_import_name})
                            
                            # Activate the target append pipeline AND ensure it is set to append
                            # This prevents data loss if the target pipeline was accidentally configured as 'replace'
                            activate_stmt = text("UPDATE elt_pipeline_configs SET is_active = 1, load_method = 'append' WHERE import_name = :target_import")
                            res = connection.execute(activate_stmt, {"target_import": target_import_to_activate})
                            if res.rowcount == 0:
                                context.log.error(f"CRITICAL: Failed to activate target import '{target_import_to_activate}': Import not found in database. Pipeline chain is broken!")
                            transaction.commit()
                    context.log.info(f"Successfully updated database. '{triggering_import_name}' is now inactive, and '{target_import_to_activate}' is active. The correct sensor will run on the next tick.")
            except Exception as e:
                log_details["error_details"] = traceback.format_exc()
                log_details["resolution_steps"] = f"Review Dagster logs for '{context.asset_key.to_user_string()}'. Inspect the SQL stored procedure '{transform_procedure}'."
                log_details["message"] = str(e)
                context.log.error(f"Error during transform for {pipeline_name}: {e}")
                raise
            finally:
                log_details["end_time"] = datetime.utcnow()
                
                # Check for downstream dependencies to suppress SUCCESS notifications
                # We only want to notify when the *final* import in a chain completes.
                suppress_success_notification = False
                if log_details.get("status") == "SUCCESS":
                    # 1. Check Batch Detection (Smart Replace)
                    # If this was a batch append, suppress notification to avoid spam.
                    if "decision_reason" in locals() and "Batch detection" in decision_reason:
                        suppress_success_notification = True
                        context.log.info("Suppressing success notification: This is a batch append run.")

                    # 2. Check Downstream Dependencies
                    if not suppress_success_notification:
                        try:
                            with engine.connect() as conn:
                                # Check if any active pipeline depends on this one
                                dep_rows = conn.execute(
                                    text("SELECT import_name, depends_on FROM elt_pipeline_configs WHERE is_active = 1 AND depends_on IS NOT NULL")
                                ).fetchall()
                                
                                for r_name, r_deps in dep_rows:
                                    if r_deps:
                                        deps_list = [d.strip().lower() for d in r_deps.split(',')]
                                        if import_name.lower() in deps_list:
                                            suppress_success_notification = True
                                            context.log.info(f"Suppressing success notification: Active pipeline '{r_name}' depends on this import.")
                                            break
                        except Exception as e:
                            context.log.warning(f"Failed to check downstream dependencies: {e}")

                if not suppress_success_notification:
                    # --- NOTIFY USER (Success/Failure) ---
                    # This is the final step of the mechanism, so we notify here.
                    # Use file_pattern as a proxy for the source file since we are in the transform step.
                    source_file_proxy = config.file_pattern or "Batch/Dependency Run"
                    
                    _show_toast_notification(
                        status=log_details["status"],
                        pipeline_name=pipeline_name,
                        import_name=import_name,
                        source_file=source_file_proxy,
                        message=log_details["message"]
                    )
                    _write_user_feedback_log(
                        monitored_directory=config.monitored_directory,
                        pipeline_name=pipeline_name,
                        import_name=import_name,
                        status=log_details["status"],
                        source_file=source_file_proxy,
                        message=log_details["message"]
                    )

                # Now that _log_asset_run is in scope, we can call it.
                _log_asset_run(engine, log_details)
        finally:
            # Release the serialization lock
            lock_conn.close()

    return transform_asset

def create_column_mapping_utility_asset(config: PipelineConfig):
    """Asset factory for creating a column mapping generation utility asset.

    This factory generates a utility asset, placed in the `_utility` group, that helps
    bootstrap new pipelines. When materialized, this asset inspects a source file and
    the target staging table to automatically generate a positional column mapping.
    This is crucial when source file headers do not match the database column names.

    Args:
        config (PipelineConfig): The configuration for the pipeline that this
            utility asset will service.

    Returns:
        A callable Dagster asset function (`generate_column_mapping_asset`).
    """
    @asset(
        name=f"{config.import_name}_generate_column_mapping",
        group_name="_utility", # A dedicated group for utility/setup assets
        compute_kind="python",
    )
    def generate_column_mapping_asset(context: AssetExecutionContext, db_resource: SQLServerResource) -> None: # No change needed, was already correct
        """Generates and saves a positional column mapping for a pipeline.

        This utility asset performs the following steps upon materialization:
        1. Finds a sample source file matching the pipeline's `file_pattern` in its
           `monitored_directory`.
        2. Reads the header row of the file to get the list of source column names.
        3. Inspects the database to get the list of column names from the target staging table.
        4. Creates a mapping string by pairing the first source column with the first table
           column, the second with the second, and so on.
        5. Updates the `column_mapping` field in the `elt_pipeline_configs` table in the
           database with the generated string.

        Args:
            context (AssetExecutionContext): The Dagster execution context.
        """
        engine = db_resource.get_engine()
        import os, fnmatch # Move import to the top of the function
        context.log.info(f"Starting column mapping generation for import: '{config.import_name}'")

        # --- 1. Get column headers from the source file ---
        inspector = inspect(engine) # Initialize the inspector here
        
        file_to_parse = None
        search_path = config.monitored_directory
        
        if not search_path or not os.path.isdir(search_path):
            raise FileNotFoundError(f"Monitored directory '{search_path}' does not exist or is not a directory. Cannot search for source file.")

        # Find the first file in the directory that matches the pattern
        for filename in os.listdir(search_path):
            if fnmatch.fnmatch(filename, config.file_pattern):
                file_to_parse = os.path.join(search_path, filename)
                context.log.info(f"Found matching source file: '{file_to_parse}'")
                break # Use the first match

        if not file_to_parse:
            raise FileNotFoundError(f"No file matching pattern '{config.file_pattern}' found in directory '{search_path}'. Cannot generate column mapping.")

        # Read the file using the appropriate parser to get the DataFrame, then get columns.
        # This ensures we can handle any file type (CSV, Excel, PSV, etc.)
        try:
            # Use the parser factory to handle different file types correctly.
            # We'll read the whole file, as some parsers (like Excel) don't support `nrows`.
            # This is acceptable for a utility asset that is run manually.
            parser = parsers.parser_factory.get_parser(config.file_type)
            context.log.info(f"Using '{config.file_type}' parser from factory to read headers from: {file_to_parse}")
            df_sample = parser.parse(file_to_parse)
            source_columns = df_sample.columns.tolist()
            context.log.info(f"Found source columns: {source_columns}")
        except Exception as e:
            context.log.error(f"Failed to parse source file '{file_to_parse}' to get headers: {e}")
            raise

        # --- 2. Get column names from the staging table ---
        try:
            table_columns = [col['name'] for col in inspector.get_columns(config.staging_table)]
            # Exclude the framework-managed columns from the mapping
            table_columns = [col for col in table_columns if col.lower() != 'dagster_run_id']
            context.log.info(f"Found staging table columns: {table_columns}")
        except Exception as e:
            context.log.error(f"Failed to inspect columns for table '{config.staging_table}': {e}")
            raise

        # --- 3. Generate the mapping string ---
        if len(source_columns) != len(table_columns):
            context.log.warning(f"Column count mismatch! Source has {len(source_columns)} columns, table has {len(table_columns)}. Mapping will be based on position.")

        mapping_pairs = []
        for i, source_col in enumerate(source_columns):
            if i < len(table_columns):
                target_col = table_columns[i]
                # Always add to mapping based on order (source_col[i] -> target_col[i])
                mapping_pairs.append(f"{source_col} > {target_col}")

        mapping_string = ", ".join(mapping_pairs)
        context.log.info(f"Generated mapping string: '{mapping_string}'")

        # --- 4. Update the database ---
        update_stmt = text("UPDATE elt_pipeline_configs SET column_mapping = :mapping WHERE import_name = :import_name")
        with engine.connect() as connection: # This was also correct.
            with connection.begin() as transaction:
                connection.execute(update_stmt, {"mapping": mapping_string, "import_name": config.import_name})
                transaction.commit()
        
        context.add_output_metadata({"generated_mapping": mapping_string, "updated_import_name": config.import_name})
        context.log.info(f"Successfully updated 'column_mapping' for '{config.import_name}' in the database.")

    return generate_column_mapping_asset

def create_ddl_generation_utility_asset(config: PipelineConfig):
    """Asset factory for creating a DDL (Data Definition Language) generation utility asset.

    This factory generates a utility asset, placed in the `_utility` group, designed to
    accelerate the setup of new pipelines. When materialized, it infers a schema
    from a sample source file and generates the `CREATE TABLE` and `CREATE PROCEDURE`
    SQL statements for the full pipeline.

    Args:
        config (PipelineConfig): The configuration for the pipeline that this
            utility asset will service.

    Returns:
        A callable Dagster asset function (`generate_ddl_asset`).
    """
    @asset(
        name=f"{config.import_name}_generate_ddl",
        group_name="_utility", # A dedicated group for utility/setup assets
        compute_kind="python",
    )
    def generate_ddl_asset(context: AssetExecutionContext) -> None:
        """Infers a schema from a source file and generates all necessary DDL.

        This utility asset performs the following steps upon materialization:
        1. Finds a sample source file matching the pipeline's `file_pattern`.
        2. Reads the first 1000 rows to infer column names and data types (e.g., INT, FLOAT, DATETIME, NVARCHAR).
        3. Constructs `CREATE TABLE` DDL for both the staging and destination tables,
           and a `CREATE OR ALTER PROCEDURE` for the transformation logic. It includes
           framework-specific columns like `dagster_run_id` and `load_timestamp`.
        4. Saves the generated SQL script to a file in the `generated_sql/` directory
           (e.g., `generated_sql/my_import_setup.sql`).
        5. Attaches the same SQL script as markdown to the asset's output metadata for easy viewing in the UI.

        Args:
            context (AssetExecutionContext): The Dagster execution context.
        """
        import os, fnmatch
        # --- 1. Find a source file to inspect ---
        file_to_parse = None
        search_path = config.monitored_directory

        if not search_path or not os.path.isdir(search_path):
            raise FileNotFoundError(f"Monitored directory '{search_path}' does not exist or is not a directory. Cannot find a file to generate DDL from.")

        for filename in os.listdir(search_path):
            if fnmatch.fnmatch(filename, config.file_pattern):
                file_to_parse = os.path.join(search_path, filename)
                context.log.info(f"Found matching source file for schema inference: '{file_to_parse}'")
                break

        if not file_to_parse:
            raise FileNotFoundError(f"No file matching pattern '{config.file_pattern}' found in directory '{search_path}'.")

        # --- 2. Infer schema from the file ---
        try:
            # Read a sample of the file to infer data types
            df_sample = pd.read_csv(file_to_parse, nrows=1000, encoding='latin1')
            context.log.info(f"Inferred schema from the first 1000 rows of '{file_to_parse}'.")
        except Exception as e:
            context.log.error(f"Failed to read and infer schema from source file '{file_to_parse}': {e}")
            raise

        def pandas_dtype_to_sql(dtype):
            """Maps pandas dtype to a reasonable SQL Server type."""
            if pd.api.types.is_integer_dtype(dtype):
                return "BIGINT"
            elif pd.api.types.is_float_dtype(dtype):
                return "FLOAT"
            elif pd.api.types.is_bool_dtype(dtype):
                return "BIT"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                return "DATETIME"
            else: # Object, string, etc.
                return "NVARCHAR(MAX)"

        # --- 3. Generate DDL for Staging Table ---
        staging_cols = []
        for col_name, dtype in df_sample.dtypes.items():
            sql_type = pandas_dtype_to_sql(dtype)
            staging_cols.append(f"    [{col_name}] {sql_type} NULL")
        
        staging_cols.append("    [dagster_run_id] NVARCHAR(255) NULL") # Add the mandatory run_id column

        staging_ddl = f"CREATE TABLE {config.staging_table} (\n" + ",\n".join(staging_cols) + "\n);"

        # --- 4. Generate DDL for Destination Table ---
        dest_cols = []
        for col_name, dtype in df_sample.dtypes.items():
            sql_type = pandas_dtype_to_sql(dtype)
            dest_cols.append(f"    [{col_name}] {sql_type} NULL")

        dest_cols.append("    [load_timestamp] DATETIME DEFAULT GETUTCDATE()") # Add a load timestamp

        dest_ddl = f"CREATE TABLE {config.destination_table} (\n" + ",\n".join(dest_cols) + "\n);"
        # Handle multiple destination tables
        dest_tables = [t.strip() for t in config.destination_table.split(',')]
        dest_ddl_parts = []
        for dt in dest_tables:
            dest_ddl_parts.append(f"CREATE TABLE {dt} (\n" + ",\n".join(dest_cols) + "\n);")
        
        dest_ddl = "\n\n".join(dest_ddl_parts)
        
        shared_columns = [f"    [{col_name}]" for col_name in df_sample.columns]
        shared_columns_str = ",\n".join(shared_columns)
        
        sp_ddl = f"""CREATE OR ALTER PROCEDURE {config.transform_procedure}
    @run_id NVARCHAR(255),
    @tables_to_truncate NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @tables_to_truncate IS NOT NULL AND LEN(@tables_to_truncate) > 0
    BEGIN
        DECLARE @sql NVARCHAR(MAX) = (SELECT 'TRUNCATE TABLE ' + QUOTENAME(T.c.value('.', 'NVARCHAR(255)')) + ';' FROM (SELECT CAST(('<t>' + REPLACE(@tables_to_truncate, ',', '</t><t>') + '</t>') AS XML)) AS A(X) CROSS APPLY A.X.nodes('/t') AS T(c) FOR XML PATH(''));
        EXEC sp_executesql @sql;
    END
    IF EXISTS (SELECT 1 FROM {config.staging_table} WHERE dagster_run_id = @run_id)
    BEGIN
        INSERT INTO {config.destination_table} ({shared_columns_str.replace("    ", "")}) SELECT {shared_columns_str.replace("    ", "")} FROM {config.staging_table} WHERE dagster_run_id = @run_id;
        INSERT INTO {dest_tables[0]} ({shared_columns_str.replace("    ", "")}) SELECT {shared_columns_str.replace("    ", "")} FROM {config.staging_table} WHERE dagster_run_id = @run_id;
    END;
END;"""
        
        # --- 5. Combine all DDL into a single markdown output ---
        full_ddl_md = f"""
## Generated DDL for '{config.import_name}'

Review and execute the following SQL statements in your database to create the necessary tables and stored procedure.

### Staging Table: `{config.staging_table}`

```sql
{staging_ddl}
```

### Destination Table: `{config.destination_table}`

```sql
{dest_ddl}
```

### Stored Procedure: `{config.transform_procedure}`

```sql
{sp_ddl}
```
"""
        context.add_output_metadata({"generated_ddl": MetadataValue.md(full_ddl_md)})
        context.log.info("Successfully generated DDL. View the asset's metadata for the SQL script.")

    return generate_ddl_asset

def create_pipeline_setup_utility_asset(pipeline_name: str, configs: List[PipelineConfig]):
    """
    Asset factory for creating a single utility asset that generates a consolidated
    SQL setup script for an entire pipeline group.

    This factory generates a single asset, e.g., `my_pipeline_generate_setup_sql`, placed in
    the `_utility` group. When materialized, this asset iterates through all active
    `PipelineConfig` objects and performs the following for each:
    1. Infers a schema from a sample source file.
    2. Generates `CREATE TABLE` DDL for staging and destination tables.
    3. Generates `CREATE OR ALTER PROCEDURE` DDL for the transformation logic.
    4. Generates a SQL `UPDATE` statement to set the positional column mapping.
    5. Combines all generated SQL into a single `.sql` file, saved to the `generated_sql/`
       directory, ready for review and execution.


    Returns:
        A callable Dagster asset function (`generate_all_setup_sql_asset`).
    """
    @asset(
        name=f"{pipeline_name.strip().lower()}_generate_setup_sql",
        group_name="_utility",
        compute_kind="python",
    )
    def generate_pipeline_setup_sql_asset(context: AssetExecutionContext, db_resource: SQLServerResource) -> None:
        """
        Generates a single SQL file containing DDL and column mapping updates for all pipelines.
        """
        import fnmatch
        inspector = inspect(db_resource.get_engine())
        output_dir = "generated_sql"
        os.makedirs(output_dir, exist_ok=True)
        output_filename = os.path.join(output_dir, "generated_full_setup.sql")
        

        all_staging_ddl = []
        all_dest_ddl = []
        sp_insert_blocks = []
        context.log.info(f"Starting bulk SQL generation for pipeline '{pipeline_name}'. Output will be saved to '{output_filename}'.")

        all_staging_ddl = []
        all_dest_ddl = []
        sp_insert_blocks = []

        full_sql_script = [f"-- Generated on {datetime.now()}\n-- This script contains DDL and column mapping updates for all configured pipelines.\n"]
        
        def pandas_dtype_to_sql(dtype):
            if pd.api.types.is_integer_dtype(dtype): return "BIGINT"
            if pd.api.types.is_float_dtype(dtype): return "FLOAT"
            if pd.api.types.is_bool_dtype(dtype): return "BIT"
            if pd.api.types.is_datetime64_any_dtype(dtype): return "DATETIME"
            return "NVARCHAR(MAX)"

        for config in configs:
            context.log.info(f"--- Processing pipeline: {config.import_name} ---")
            try:
                # --- 1. Find a source file to inspect ---
                file_to_parse = None
                search_path = config.monitored_directory
                if not search_path or not os.path.isdir(search_path):
                    context.log.warning(f"Skipping '{config.import_name}': Monitored directory '{search_path}' does not exist.")
                    continue

                for filename in os.listdir(search_path):
                    if fnmatch.fnmatch(filename, config.file_pattern):
                        file_to_parse = os.path.join(search_path, filename)
                        break
                
                if not file_to_parse:
                    context.log.warning(f"Skipping '{config.import_name}': No file matching pattern '{config.file_pattern}' found in '{search_path}'.")
                    continue

                context.log.info(f"Found sample file: {file_to_parse}")

                # --- 2. Infer schema and generate DDL ---
                df_sample = pd.read_csv(file_to_parse, nrows=1000, encoding='latin1')
                source_columns = df_sample.columns.tolist()

                # Staging Table DDL
                staging_cols = [f"    [{col}] {pandas_dtype_to_sql(dtype)} NULL" for col, dtype in df_sample.dtypes.items()]
                staging_cols.append("    [dagster_run_id] NVARCHAR(255) NULL")
                all_staging_ddl.append(f"-- Staging table for {config.import_name}\nCREATE TABLE {config.staging_table} (\n" + ",\n".join(staging_cols) + "\n);")

                # Destination Table DDL
                dest_cols = [f"    [{col}] {pandas_dtype_to_sql(dtype)} NULL" for col, dtype in df_sample.dtypes.items()]
                dest_cols.append("    [load_timestamp] DATETIME DEFAULT GETUTCDATE()")
                all_dest_ddl.append(f"-- Destination table for {config.import_name}\nCREATE TABLE {config.destination_table} (\n" + ",\n".join(dest_cols) + "\n);")

                # Stored Procedure INSERT block
                shared_cols = ",\n".join([f"        [{col}]" for col in df_sample.columns])
                insert_block = f"""
    -- Logic for import: {config.import_name}
    IF EXISTS (SELECT 1 FROM {config.staging_table} WHERE dagster_run_id = @run_id)
    BEGIN
        INSERT INTO {config.destination_table} (
{shared_cols.replace("    ", "")}
        ) 
        SELECT 
{shared_cols}
        FROM {config.staging_table} WHERE dagster_run_id = @run_id;
    END;"""
                sp_insert_blocks.append(insert_block)

            except Exception as e:
                context.log.error(f"Failed to generate DDL for import '{config.import_name}': {e}")
                all_staging_ddl.append(f"-- FAILED to generate DDL for import: {config.import_name}. Error: {e}")

        # --- 3. Assemble the single stored procedure for the pipeline ---
        transform_procedure = configs[0].transform_procedure
        sp_body = "\n".join(sp_insert_blocks)
        sp_ddl = f"""CREATE OR ALTER PROCEDURE {transform_procedure}
    @run_id NVARCHAR(255),
    @tables_to_truncate NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    -- Truncation logic
    IF @tables_to_truncate IS NOT NULL AND LEN(@tables_to_truncate) > 0
    BEGIN
        DECLARE @sql NVARCHAR(MAX) = (SELECT 'TRUNCATE TABLE ' + QUOTENAME(T.c.value('.', 'NVARCHAR(255)')) + ';' FROM (SELECT CAST(('<t>' + REPLACE(@tables_to_truncate, ',', '</t><t>') + '</t>') AS XML)) AS A(X) CROSS APPLY A.X.nodes('/t') AS T(c) FOR XML PATH(''));
        EXEC sp_executesql @sql;
    END
{sp_body}
END;"""

        # --- 4. Combine everything into a single markdown output ---
        # Precompute joinedAl kPo avobksashes inside f-string expressions
        staging_ddl_joined = ("\nGO\n\n".join(all_staging_ddl) + "\nGO") if all_staging_ddl else ""
        dest_ddl_joined = ("\nGO\n\n".join(all_dest_ddl) + "\nGO") if all_dest_ddl else ""

        full_ddl_md = f"""
## Generated SQL Setup for All Pipelines

### Staging Tables
```sql
{staging_ddl_joined}
```

### Destination Tables
```sql
{dest_ddl_joined}
```

### Consolidated Stored Procedure
```sql
{sp_ddl}
GO
```
"""
        context.add_output_metadata({"generated_sql_setup": MetadataValue.md(full_ddl_md)})
        context.log.info(f"✅ Successfully generated consolidated DDL for pipeline '{pipeline_name}'.")

    return generate_pipeline_setup_sql_asset


def create_pipeline_column_mapping_utility_asset(pipeline_name: str, configs: List[PipelineConfig]):
    """
    Asset factory for creating a single, consolidated column mapping utility asset per pipeline.

    This factory generates one asset per pipeline group (e.g., 'my_pipeline_generate_column_mappings')
    that, when materialized, generates and saves positional column mappings for every import
    within that pipeline, but only if their corresponding staging tables already exist.

    Args:
        pipeline_name (str): The name of the pipeline group.
        configs (List[PipelineConfig]): The list of configurations for all imports in this pipeline.

    Returns:
        A callable Dagster asset function.
    """
    pipeline_group_name = pipeline_name.strip().lower()

    @asset(
        name=f"{pipeline_group_name}_generate_column_mappings",
        group_name="_utility",
        compute_kind="python",
        description=f"Generates and saves column mappings for all imports in the '{pipeline_name}' pipeline."
    )
    def pipeline_column_mapping_asset(context: AssetExecutionContext, db_resource: SQLServerResource) -> None:
        """
        Generates and saves positional column mappings for all imports in a pipeline.
        This asset will only generate a mapping for an import if its staging table exists.
        """
        engine = db_resource.get_engine()
        inspector = inspect(engine)
        context.log.info(f"Starting consolidated column mapping generation for pipeline: '{pipeline_name}'")

        markdown_output = [f"## Generated Mappings for Pipeline: '{pipeline_name}'\n"]

        for config in configs:
            context.log.info(f"--- Processing import: {config.import_name} ---")
            
            if not inspector.has_table(config.staging_table):
                context.log.warning(f"Staging table '{config.staging_table}' does not exist. Skipping column mapping for this import. Please run the DDL setup asset first.")
                continue

            try:
                # --- 1. Find a source file to inspect ---
                import os, fnmatch
                file_to_parse = None
                search_path = config.monitored_directory
                if not search_path or not os.path.isdir(search_path):
                    raise FileNotFoundError(f"Monitored directory '{search_path}' does not exist.")

                for filename in os.listdir(search_path):
                    if fnmatch.fnmatch(filename, config.file_pattern):
                        file_to_parse = os.path.join(search_path, filename)
                        break
                
                if not file_to_parse:
                    raise FileNotFoundError(f"No file matching pattern '{config.file_pattern}' found in '{search_path}'.")

                # --- 2. Get source and table columns ---
                parser = parsers.parser_factory.get_parser(config.file_type)
                df_sample = parser.parse(file_to_parse)
                source_columns = df_sample.columns.tolist()

                table_columns = [col['name'] for col in inspector.get_columns(config.staging_table) if col['name'].lower() != 'dagster_run_id']

                # --- 3. Generate and save mapping ---
                mapping_pairs = [f"{source_col} > {table_columns[i]}" for i, source_col in enumerate(source_columns) if i < len(table_columns)]
                mapping_string = ", ".join(mapping_pairs)

                update_stmt = text("UPDATE elt_pipeline_configs SET column_mapping = :mapping WHERE import_name = :import_name")
                with engine.connect() as connection: # This was also correct.
                    with connection.begin() as transaction:
                        connection.execute(update_stmt, {"mapping": mapping_string, "import_name": config.import_name})
                        transaction.commit()
                
                markdown_output.append(f"### Import: `{config.import_name}`")
                markdown_output.append("```")
                markdown_output.append(mapping_string)
                markdown_output.append("```")
                context.log.info(f"✅ Successfully generated and saved mapping for '{config.import_name}'.")

            except Exception as e:
                context.log.error(f"Failed to generate mapping for import '{config.import_name}': {e}")

        context.add_output_metadata({"generated_mappings": MetadataValue.md("\n".join(markdown_output))})
        context.log.info(f"Finished column mapping generation for pipeline '{pipeline_name}'.")

    return pipeline_column_mapping_asset


def create_backup_utility_asset():
    """
    Asset factory for creating a database object backup utility.

    This factory generates a single utility asset that, when materialized, connects to the
    database and scripts out the data and definitions of critical objects. This serves as
    a simple, file-based backup mechanism for disaster recovery or versioning.
    """
    @asset(
        name="utility_backup_database_objects",
        group_name="_utility",
        compute_kind="python",
    )
    def backup_database_objects_asset(context: AssetExecutionContext, db_resource: SQLServerResource) -> None:
        """
        Scripts critical database tables and stored procedures to .sql files for backup.

        This asset performs the following steps:
        1. Defines a list of critical tables to back up (configs, logs, governance).
        2. Queries the `elt_pipeline_configs` table to dynamically find all unique
           stored procedures used for transformations.
        3. For each table, it reads all data into a pandas DataFrame and generates
           SQL `INSERT` statements for every row.
        4. For each stored procedure, it queries system views to get its full definition.
        5. Saves the generated SQL scripts to a `backups/` directory in the project root.

        Args:
            context (AssetExecutionContext): The Dagster execution context.
        """
        engine = db_resource.get_engine()
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True)
        context.log.info(f"Starting database object backup to directory: '{backup_dir}'")

        backed_up_files = []

        # --- 1. Define tables to back up data from ---
        tables_to_backup = [
            "elt_pipeline_configs",
            "etl_pipeline_run_logs",
            "data_quality_rules",
            "data_quality_run_logs"
        ]

        def _sanitize_sql_string(value: any) -> str:
            """Sanitizes a value for use in a SQL string literal."""
            # First, convert the value to a string.
            s = str(value)
            # Then, escape single quotes.
            s = s.replace("'", "''")
            # Finally, escape backslashes.
            s = s.replace("\\", "\\\\")
            return s

        # --- 2. Back up table data as INSERT statements ---
        for table_name in tables_to_backup:
            try:
                context.log.info(f"Backing up data from table: {table_name}")
                df = pd.read_sql_table(table_name, engine)

                if df.empty:
                    context.log.info(f"Table '{table_name}' is empty, skipping.")
                    continue

                # Generate INSERT statements
                output_filename = os.path.join(backup_dir, f"backup_data_{table_name}.sql")
                with open(output_filename, 'w', encoding='utf-8') as f:
                    f.write(f"-- Data backup for {table_name} on {datetime.now()}\n")
                    f.write(f"TRUNCATE TABLE {table_name};\nGO\n\n")
                    f.write(f"SET IDENTITY_INSERT {table_name} ON;\nGO\n\n")

                    for _, row in df.iterrows():
                        cols = ", ".join([f"[{c}]" for c in df.columns])
                        
                        vals_list = []
                        for val in row:
                            if pd.isna(val):
                                vals_list.append("NULL")
                            else:
                                sanitized_val = _sanitize_sql_string(val)
                                # Use standard string concatenation to avoid the f-string parser bug with backslashes.
                                # By creating the final string in a separate variable before appending,
                                # we make the code's intent unambiguous to the Python parser.
                                # Sanitize the value and wrap it in N'' for SQL Server.
                                final_val = "N'" + sanitized_val + "'"
                                vals_list.append(final_val)
                        vals = ", ".join(vals_list)

                        f.write(f"INSERT INTO {table_name} ({cols}) VALUES ({vals});\n")
                    
                    f.write(f"\nSET IDENTITY_INSERT {table_name} OFF;\nGO\n")

                backed_up_files.append(output_filename)
                context.log.info(f"Successfully backed up {len(df)} rows from '{table_name}' to '{output_filename}'.")

            except Exception as e:
                context.log.error(f"Failed to back up table {table_name}: {e}")

        context.add_output_metadata({"backed_up_files": MetadataValue.md("\n".join([f"- `{f}`" for f in backed_up_files]))})

    return backup_database_objects_asset
