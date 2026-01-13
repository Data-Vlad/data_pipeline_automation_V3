# c:\Users\Staff\Dropbox\Projects\Work\data_pipeline_automation\elt_project\core\sql_loader.py
import pandas as pd
from sqlalchemy import text
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

def load_df_to_sql(df: pd.DataFrame, table_name: str, engine: Engine):
    """
    Loads a DataFrame into a SQL table by appending.
    Truncation for 'replace' load method is now handled in the asset factory.
    """
    with engine.connect() as connection:
        # Use a transaction to ensure atomicity
        with connection.begin() as transaction:
            try:
                # Use pandas to_sql for efficient bulk loading
                df.to_sql(
                    name=table_name,
                    con=connection,
                    if_exists="append",
                    index=False,
                    chunksize=1000, # Adjust chunksize based on memory and table width
                )
                transaction.commit()
            except Exception as e:
                transaction.rollback()
                raise e

def load_csv_to_sql_chunked(
    file_path: str,
    table_name: str,
    engine: Engine,
    run_id: str,
    column_mapping: dict = None,
    chunksize: int = 10000,
):
    """
    Reads a large CSV file in chunks and loads each chunk into a SQL table.
    This is highly memory-efficient as the entire file is never loaded into memory.
    Truncation for 'replace' load method is now handled in the asset factory.
    Returns the total number of rows processed.
    """
    total_rows = 0
    with engine.connect() as connection:
        with connection.begin() as transaction:
            try:
                # Use pandas.read_csv with a chunksize to create an iterator.
                with pd.read_csv(
                    file_path,
                    chunksize=chunksize,
                    encoding='latin1', # Added encoding for broader compatibility
                    true_values=['true', 'True', 'TRUE', '1'],
                    false_values=['false', 'False', 'FALSE', '0', ''],
                    dtype=str # Treat all columns as strings to prevent incorrect type inference
                ) as reader:
                    for chunk in reader:
                        # Apply column mapping to each chunk
                        if column_mapping:
                            # Make a copy to avoid SettingWithCopyWarning
                            chunk = chunk.rename(columns=column_mapping)

                        chunk['dagster_run_id'] = run_id

                        # Find boolean-like columns and fill NaNs with 0 (False)
                        # This mirrors the logic in the non-chunked path in factory.py
                        bool_cols = [col for col in chunk.columns if 'checkbox' in col.lower() or 'canbecompleted' in col.lower()]
                        if bool_cols:
                            # Ensure we are modifying the copied chunk if it exists
                            chunk.loc[:, bool_cols] = chunk[bool_cols].fillna(0)

                        total_rows += len(chunk)
                        chunk.to_sql(name=table_name, con=connection, if_exists='append', index=False)
                
                transaction.commit()
                return total_rows
            except Exception as e:
                transaction.rollback()
                raise e


def execute_stored_procedure(
    procedure_name: str,
    engine: Engine,
    run_id: str = None,
    tables_to_truncate: str = None
):
    """
    Executes a stored procedure in the database.
    Optionally passes a run_id and a list of tables to truncate to the stored procedure.
    """
    with engine.connect() as connection:
        with connection.begin() as transaction:
            try:
                # Build the execution text and parameters
                # The procedure name must be part of the string and not a parameter.
                # Since it comes from a trusted config, this is safe.
                exec_text = f"EXEC {procedure_name}"
                params = {}
                
                # Append parameters, ensuring commas are used to separate them if needed.
                param_parts = []
                if run_id:
                    param_parts.append("@run_id = :run_id")
                    params["run_id"] = run_id
                if tables_to_truncate:
                    param_parts.append("@tables_to_truncate = :tables_to_truncate")
                    params["tables_to_truncate"] = tables_to_truncate
                if param_parts:
                    exec_text += " " + ", ".join(param_parts)
                connection.execute(text(exec_text), params)
                transaction.commit()
            except Exception as e:
                transaction.rollback()
                raise e
