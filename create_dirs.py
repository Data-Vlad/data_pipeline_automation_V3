import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def create_monitored_directories():
    """
    Connects to the database, reads the elt_pipeline_configs table,
    and creates any `monitored_directory` paths that do not exist.
    """
    try:
        # This script is called from the project root by the batch file,
        # so .env should be in the current working directory.
        load_dotenv()

        # Read connection details from environment variables set by the .env file.
        server = os.getenv("DB_SERVER")
        database = os.getenv("DB_DATABASE")
        user = os.getenv("DB_USERNAME", "").strip() # Set by get_credentials.py and stripped for safety
        password = os.getenv("DB_PASSWORD", "").strip() # Set by get_credentials.py and stripped for safety

        if not all([server, database, user, password]):
            raise ValueError("One or more database environment variables (DB_SERVER, DB_DATABASE, DB_USERNAME, DB_PASSWORD) are not set.")

        # Build the connection string directly for SQLAlchemy.
        # If DB_DRIVER is not set or is empty, use the default and format it.
        # The driver name should be URL-encoded (spaces to '+') for the SQLAlchemy URL.
        # If the driver name from the environment variable includes curly braces, remove them first,
        # as SQLAlchemy's mssql+pyodbc dialect expects the raw driver name for URL encoding.
        db_driver_raw = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
        # Robustly clean the driver name: remove quotes, then braces, then URL-encode spaces.
        driver = db_driver_raw.strip('"').strip('{}').replace(' ', '+')
        conn_string = (
            f"mssql+pyodbc://{user}:{password}@{server}/{database}"
            f"?driver={driver}&TrustServerCertificate=yes"
        )

        engine = create_engine(conn_string)

        with engine.connect() as connection: # This was already correct.
            query = text(
                "SELECT DISTINCT monitored_directory FROM elt_pipeline_configs "
                "WHERE monitored_directory IS NOT NULL AND is_active = 1"
            )
            results = connection.execute(query).fetchall()

        if not results:
            print("      No active, monitored directories found in the configuration table.")
            return

        for (dir_path,) in results:
            # Ensure dir_path is a non-empty string before processing
            if not isinstance(dir_path, str) or not dir_path.strip():
                continue
            
            path = Path(dir_path.strip())
            print(f"      Ensuring directory exists: {path}")
            path.mkdir(parents=True, exist_ok=True)

    except Exception as e:
        print(f"ERROR: An error occurred while creating directories: {e}", file=sys.stderr)
        # Exit with a non-zero status code to signal failure to the calling batch script.
        sys.exit(1)

if __name__ == "__main__":
    create_monitored_directories()