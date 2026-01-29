import os
import argparse
import sys
import threading
import re
import logging
from collections import defaultdict
from typing import List, Optional, Any
from dotenv import load_dotenv
from dagster import DagsterInstance, DagsterRunStatus
from dagster._core.utils import make_new_run_id
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import WorkspaceFileTarget
from flask import Flask, g, jsonify, make_response, render_template, request
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool


# --- Python Path Correction ---
project_root = os.path.dirname(__file__)
# Add the project root to the path so that 'elt_project' can be imported as a package.
if project_root not in sys.path:
    sys.path.append(project_root)

# --- Flask App Initialization ---
# This must be done BEFORE any other imports that might touch Dagster,
# and before the Flask app is created.
dagster_home_path = os.path.join(os.path.dirname(__file__), 'dagster_home')
if not os.path.exists(dagster_home_path):
    os.makedirs(dagster_home_path)
os.environ['DAGSTER_HOME'] = dagster_home_path

# --- Logging Configuration ---
# Configure a logger to provide detailed error messages with tracebacks.
# This will replace all `print(..., file=sys.stderr)` calls for errors.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    # Log directly to simple_ui.log instead of stdout to avoid duplicate logs (e.g. ui-server.log)
    handlers=[logging.FileHandler('simple_ui.log')]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.template_folder = 'templates'
app.secret_key = os.urandom(24)

# Load environment variables from .env file
load_dotenv()

# --- Command-Line Argument Parsing ---
parser = argparse.ArgumentParser(description="Data Importer UI")
parser.add_argument("--server", required=True, help="Database server name")
parser.add_argument("--database", required=True, help="Database name")
parser.add_argument("--credential-target", required=True, help="The target name for Windows Credential Manager")
cli_args = parser.parse_args()

# --- Application State Management ---
# This class will hold our long-lived objects and track the app's initialization status.
# This is a more robust, thread-safe pattern than using global variables directly.
class AppState:
    def __init__(self):
        self.db_engine = None
        self.initialization_status = "PENDING"  # PENDING, SUCCESS, or FAILED
        self.initialization_error = None
        self.lock = threading.Lock()

APP_STATE = AppState()


def _initialize_app_thread():
    """
    Runs the app's initialization logic in a background thread.
    This prevents a slow or failed connection from blocking the server from starting.
    Updates the global APP_STATE with the result.
    """
    try:
        logger.info("=" * 60)
        logger.info("--- Application Initialization Started ---")

        # --- Step 1: Retrieve Database Credentials ---
        logger.info("INIT(1/3): Retrieving database credentials from environment...")
        # These are set by the launcher script. Using a 'DAGSTER_' prefix is a good convention.
        username = os.getenv("DAGSTER_DB_USERNAME", "").strip()
        password = os.getenv("DAGSTER_DB_PASSWORD", "").strip()

        if not username:
            raise ValueError(
                "Database username was not provided by the launcher."
            )
        if not password:
            raise ValueError(
                f"Database password for target '{cli_args.credential_target}' could not be retrieved."
            )
        logger.info("INIT(1/3): Successfully retrieved database credentials.")

        # --- Step 2: Test Database Connection ---
        logger.info("INIT(2/3): Testing database connection...")
        _test_db_connection(username, password)
        logger.info("INIT(2/3): Database connection test successful.")

        # --- Step 3: Verify Dagster Workspace ---
        logger.info("[3/3] Verifying Dagster workspace configuration...")
        workspace_file_path = os.path.join(dagster_home_path, "workspace.yaml")
        if not os.path.exists(workspace_file_path):
            raise FileNotFoundError(
                f"Dagster workspace file not found at '{workspace_file_path}'. "
            )
        logger.info("[3/3] Dagster workspace file found.")

        # --- Success! Mark app as ready. ---
        with APP_STATE.lock:
            APP_STATE.initialization_status = "SUCCESS"
        logger.info("--- Application Initialization Succeeded ---")
        logger.info("=" * 60)

    except Exception as e:
        # --- Failure: Update global state with error info ---
        # Log the full exception traceback for detailed debugging.
        logger.critical(
            "--- Application Initialization FAILED ---",
            exc_info=True,
        )
        # Add a clear resolution block to the logs.
        logger.critical("-" * 60)
        logger.critical(f"[RESOLUTION] Startup failed due to: {e}")
        logger.critical("Please check the traceback above and review the launcher logs ('launcher-error.log') for related errors.")
        logger.critical("-" * 60)
        with APP_STATE.lock:
            APP_STATE.initialization_status = "FAILED"
            # Provide a more user-friendly error message that includes the resolution steps.
            APP_STATE.initialization_error = f"Startup failed: {e}"


def _test_db_connection(username, password):
    """Create a temporary engine to test the DB connection, then discard it."""
    logger.info("DB      : Creating temporary engine to test connection...")
    db_driver_raw = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    driver = db_driver_raw.strip('"').strip('{}').replace(' ', '+')
    conn_string = (
        f"mssql+pyodbc://{username}:{password}@{cli_args.server}/{cli_args.database}"
        f"?driver={driver}&TrustServerCertificate=yes"
    )
    # Create a temporary engine with no pooling to test the connection.
    temp_engine = create_engine(conn_string, poolclass=NullPool, connect_args={"timeout": 10})
    try:
        with temp_engine.connect():
            logger.info("DB      : Connection test successful.")
    except Exception as e:
        # This provides a highly specific error message if the connection itself fails.
        logger.error(
            f"DB      : Connection test failed for server '{cli_args.server}' and database '{cli_args.database}'.",
            exc_info=True,
        )
        logger.error(
            "[RESOLUTION] Check: 1) DB server is running. 2) .env server/db names are correct. 3) Network/firewall allows connection. 4) Credentials are correct."
        )
        raise e # Re-raise the exception to be caught by the main initializer.
    finally:
        # Ensure the temporary engine is disposed of.
        temp_engine.dispose()


def _get_db_engine():
    """
    Ensures the SQLAlchemy engine is initialized and returns it.
    The engine is stored globally in APP_STATE.db_engine.
    """
    # This is the key fix: The engine is created on the first request,
    # ensuring it is created in the correct thread context for the web server.
    with APP_STATE.lock:
        if APP_STATE.db_engine is None:
            logger.info("DB      : Initializing persistent, thread-safe database engine...")
            password = os.getenv("DAGSTER_DB_PASSWORD", "").strip()
            username = os.getenv("DAGSTER_DB_USERNAME", "").strip()
            db_driver_raw = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
            driver = db_driver_raw.strip('"').strip('{}').replace(' ', '+')
            conn_string = (
                f"mssql+pyodbc://{username}:{password}@{cli_args.server}/{cli_args.database}"
                f"?driver={driver}&TrustServerCertificate=yes"
            )
            # pool_pre_ping checks connection validity; connect_args prevents hangs on new connections.
            # A 10-second timeout is a reasonable default.
            APP_STATE.db_engine = create_engine(conn_string, pool_pre_ping=True, connect_args={"timeout": 10})
            logger.info("DB      : Database engine initialized successfully.")
    return APP_STATE.db_engine


def _recreate_db_engine():
    """Disposes of the old engine and creates a new one."""
    with APP_STATE.lock:
        if APP_STATE.db_engine:
            logger.warning("DB      : Disposing of existing database engine to force reconnection.")
            APP_STATE.db_engine.dispose()
        APP_STATE.db_engine = None  # Force re-initialization on next call
    return _get_db_engine()


def get_db_connection():
    """
    Returns a database connection for the current request.
    The connection is stored in Flask's `g` object and closed automatically
    at the end of the request.
    """
    if 'db_conn' not in g:
        engine = _get_db_engine()
        g.db_conn = engine.connect()
    return g.db_conn


@app.teardown_appcontext
def close_db_connection(exception):
    """Closes the database connection at the end of the request."""
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        # This is very verbose, so we can comment it out unless debugging connection pool issues.
        # logger.info(f"DB      : Closing connection for request {request.path}")
        db_conn.close()


@app.before_request
def check_initialization():
    """
    Before every request, check if the app is initialized.
    This middleware protects all endpoints from being accessed before the app is ready.
    """
    # Allow access to status/shutdown endpoints regardless of state
    logger.info(f"--> {request.method} {request.path}")
    if request.path in ["/status", "/api/status", "/api/shutdown"]:
        return

    with APP_STATE.lock:
        if APP_STATE.initialization_status == "PENDING":
            return render_template("status.html", auto_refresh=True)
        if APP_STATE.initialization_status == "FAILED":
            return render_template("error.html", error_message=APP_STATE.initialization_error), 500
        # If SUCCESS, proceed to the requested endpoint.

@app.after_request
def log_response(response):
    """Log the status code of the response for each request."""
    logger.info(f"<-- {request.method} {request.path} - {response.status}")
    return response


@app.route("/status")
def status_page():
    """Renders the status page."""
    return render_template("status.html", auto_refresh=True)


@app.route("/api/status")
def api_status():
    """API endpoint for the frontend to poll the app's initialization status."""
    with APP_STATE.lock:
        return jsonify(
            {"status": APP_STATE.initialization_status, "error": APP_STATE.initialization_error}
        )

@app.route("/", methods=["GET"])
def index():
    """Renders the main UI page."""
    # The before_request handler ensures this only runs after successful initialization.
    response = make_response(render_template("index.html"))
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.route("/favicon.ico")
def favicon():
    """
    Handles the browser's request for a favicon.
    Returns a 204 No Content response to prevent 404 errors in the logs.
    """
    return "", 204

@app.route("/api/pipelines")
def get_pipelines():
    """
    API endpoint to fetch all active pipelines and their associated imports from the database.
    This data is used to populate the main checklist on the UI, with a clear separation
    between file-based imports and web/SFTP scrapers.
    """
    logger.info("API     : Fetching pipeline configurations from database...")
    
    # Use a defaultdict to easily group imports under their parent pipeline.
    # We now have separate lists for file-based imports and scrapers.
    pipeline_groups = defaultdict(lambda: {
        "load_imports": [], 
        "ingest_imports": [], 
        "monitored_directory": None
    })

    for attempt in range(2):  # Allow one retry
        try:
            conn = get_db_connection()
            # Query to get all active pipeline configurations.
            query = text("""
                SELECT pipeline_name, import_name, monitored_directory, parser_function
                FROM elt_pipeline_configs
                WHERE is_active = 1
                ORDER BY pipeline_name, import_name
            """)
            results = conn.execute(query).fetchall()  # Eagerly fetch all results

            # Group the flat SQL results into a nested structure with separation.
            for row in results:  # Iterate over the fetched results
                is_scraper = row.parser_function in [
                    'generic_selenium_scraper', 
                    'generic_web_scraper', 
                    'generic_sftp_downloader'
                ]
                
                import_data = {"import_name": row.import_name}

                if is_scraper:
                    pipeline_groups[row.pipeline_name]["ingest_imports"].append(import_data)
                else:
                    pipeline_groups[row.pipeline_name]["load_imports"].append(import_data)

                if row.monitored_directory:
                    pipeline_groups[row.pipeline_name]["monitored_directory"] = os.path.normpath(
                        row.monitored_directory.strip()
                    )

            # Convert the grouped data into a list format for the JSON response.
            # Filter out any pipeline groups that have no imports at all.
            pipelines = [
                {
                    "pipeline_name": pipeline_name,
                    "load_imports": data["load_imports"],
                    "etl_imports": data["load_imports"], # Alias for frontend compatibility
                    "ingest_imports": data["ingest_imports"],
                    "ingestion_imports": data["ingest_imports"], # Alias for frontend compatibility
                    "monitored_directory": data["monitored_directory"]
                }
                for pipeline_name, data in pipeline_groups.items()
                if data["load_imports"] or data["ingest_imports"]
            ]
            
            logger.info(f"API     : Successfully fetched and processed {len(pipelines)} pipelines.")
            # The frontend error "pipelines.forEach is not a function" indicates it expects a raw list.
            return jsonify(pipelines)

        except Exception as e:
            logger.warning(
                f"API     : Attempt {attempt + 1} failed to fetch pipelines.", exc_info=True
            )
            if attempt == 0:  # If this was the first attempt
                logger.info("API     : Recreating database engine and retrying...")
                _recreate_db_engine()
                # The loop will now continue to the second attempt
            else:  # If this was the second attempt
                logger.error("API     : Failed to fetch pipeline configurations from the database after retrying.", exc_info=True)
                logger.error(
                    "[RESOLUTION] This indicates a persistent DB connection problem. Verify the database is running and the 'elt_pipeline_configs' table is accessible."
                )
                return jsonify({"error": "An internal error occurred while fetching pipeline data."}), 500

    # This line should not be reachable, but is here as a fallback.
    return jsonify({"error": "An unexpected error occurred in get_pipelines."}), 500

def sanitize_name(name: str) -> str:
    """Sanitizes a string to be a valid Dagster asset name."""
    return re.sub(r'[^A-Za-z0-9_]', '_', name)

@app.route("/api/run_imports", methods=["POST"])
def run_imports():
    """
    API endpoint to trigger Dagster job runs for the imports selected by the user.
    """
    try:
        data = request.json
        selected_imports = data.get("imports", [])
        if not selected_imports:
            logger.warning("API     : 'run_imports' called with no imports selected.")
            return jsonify({"error": "No imports selected"}), 400

        import_names = [item['import_name'] if isinstance(item, dict) else item for item in selected_imports]
        logger.info(f"API     : Received request to run {len(selected_imports)} imports: {import_names}")

        # --- Fix for Asset Renaming ---
        # Identify "Ingestion" imports (scrapers) to patch their run_config keys.
        # The sensors might still generate config for the old name '_extract_and_load_staging',
        # but the assets are now named '_ingest'.
        ingestion_imports = set()
        try:
            conn = get_db_connection()
            query = text("SELECT import_name, parser_function FROM elt_pipeline_configs WHERE is_active = 1")
            results = conn.execute(query).fetchall()
            
            INGESTION_FUNCTIONS = {
                "generic_web_scraper",
                "generic_selenium_scraper",
                "generic_sftp_downloader"
            }
            
            for row in results:
                if row.import_name in import_names and row.parser_function in INGESTION_FUNCTIONS:
                    ingestion_imports.add(row.import_name)
        except Exception as e:
            logger.warning(f"API     : Failed to check for ingestion types. Config patching may be skipped. Error: {e}")

        workspace_file_path = os.path.join(dagster_home_path, "workspace.yaml")
        instance = DagsterInstance.get()
        with WorkspaceProcessContext(
            # Use DagsterInstance.get() which respects the DAGSTER_HOME environment variable.
            # This creates a non-ephemeral instance that can be used for cross-process
            # communication, which is required by WorkspaceProcessContext.
            instance=instance,
            workspace_load_target=WorkspaceFileTarget(paths=[workspace_file_path]),
        ) as process_context:
            request_context = process_context.create_request_context()
            location_name = "elt_project"
            code_location = request_context.get_code_location(location_name)

            if not code_location:
                # This is a critical failure, so we log it as an error and raise an exception.
                error_msg = f"Could not find Dagster code location '{location_name}'."
                logger.error(f"API     : {error_msg} [RESOLUTION] Check 'dagster_home/workspace.yaml' for a correct 'module_name' and ensure 'definitions.py' exists. Run 'dagster dev' to debug code loading.")
                raise RuntimeError(error_msg)

            repositories = code_location.get_repositories()
            if not repositories:
                raise RuntimeError(f"No repositories found in code location '{location_name}'. Check your definitions file.")
            
            # Get the ExternalRepository object (wrapper)
            external_repo = list(repositories.values())[0]
            logger.info(f"API     : Using repository '{external_repo.name}' from location '{location_name}'")

            results = []

            for item in selected_imports:
                if isinstance(item, dict):
                    import_name = item["import_name"]
                else:
                    import_name = item
                sensor_name = f"sensor_{import_name}"
                logger.info(f"API     :  - Submitting tick for sensor '{sensor_name}'...")

                # Default result object for this import
                import_result = {
                    "import_name": import_name,
                    "status": "skipped",
                    "run_id": None,
                    "error": None
                }

                try:
                    if not external_repo.has_sensor(sensor_name):
                        raise RuntimeError(f"Sensor '{sensor_name}' not found in repository.")

                    external_sensor = external_repo.get_sensor(sensor_name)

                    # 1. Tick the sensor to get RunRequests (this does not launch them automatically)
                    tick_result = code_location.get_sensor_execution_data(
                        instance=instance,
                        repository_handle=external_repo.handle,
                        name=sensor_name,
                        last_tick_completion_time=None,
                        last_run_key=None,
                        cursor=None,
                        log_key=[sensor_name],
                        last_sensor_start_time=None,
                    )
                    
                    # 2. Process the results and launch the actual runs
                    if not tick_result.run_requests:
                        logger.info(f"API     :  - Sensor '{sensor_name}' ticked but produced no run requests (conditions not met).")
                        import_result["error"] = "Conditions not met (no new files?)"
                        results.append(import_result)
                        continue

                    for run_req in tick_result.run_requests:
                        # --- Patch Config for Ingestion Assets ---
                        if import_name in ingestion_imports:
                            old_op_name = sanitize_name(f"{import_name}_extract_and_load_staging")
                            new_op_name = sanitize_name(f"{import_name}_ingest")
                            
                            if run_req.run_config and 'ops' in run_req.run_config:
                                ops_config = run_req.run_config['ops']
                                if old_op_name in ops_config:
                                    logger.info(f"API     : Patching run_config. Renaming '{old_op_name}' to '{new_op_name}'.")
                                    ops_config[new_op_name] = ops_config.pop(old_op_name)
                        # -----------------------------------------

                        # Resolve the job name (sensor might target a specific job or be dynamic)
                        job_name = run_req.job_name
                        if not job_name:
                            targets = external_sensor.targets
                            if targets:
                                job_name = targets[0].job_name
                        
                        if not job_name:
                            logger.warning(f"API     :  - Skipping request from '{sensor_name}': Could not resolve target job name.")
                            continue

                        # Create and submit the run
                        external_job = external_repo.get_full_job(job_name)
                        
                        # Prepare tags, ensuring run_key is preserved if present
                        run_tags = run_req.tags if run_req.tags else {}
                        if run_req.run_key:
                            run_tags["dagster/run_key"] = run_req.run_key

                        logger.info(f"API     :  - Run Config: {run_req.run_config}")

                        # Generate Execution Plan
                        execution_plan = code_location.get_execution_plan(
                            remote_job=external_job,
                            run_config=run_req.run_config or {},
                            step_keys_to_execute=None,
                            known_state=None,
                            instance=instance,
                        )

                        run_id = make_new_run_id()

                        run = instance.create_run(
                            job_name=job_name,
                            run_id=run_id,
                            run_config=run_req.run_config or {},
                            resolved_op_selection=None,
                            step_keys_to_execute=None,
                            status=DagsterRunStatus.NOT_STARTED,
                            op_selection=None,
                            root_run_id=None,
                            parent_run_id=None,
                            tags=run_tags,
                            job_snapshot=external_job.job_snapshot,
                            execution_plan_snapshot=execution_plan.execution_plan_snapshot,
                            parent_job_snapshot=external_job.parent_job_snapshot,
                            remote_job_origin=external_job.get_remote_origin(),
                            job_code_origin=external_job.get_python_origin(),
                            asset_selection=None,
                            asset_check_selection=None,
                            asset_graph=None
                        )
                        instance.launch_run(run.run_id, workspace=request_context)
                        logger.info(f"API     :  - Successfully launched run '{run.run_id}' for job '{job_name}'.")
                        
                        # Success! Create a specific result entry for this run
                        success_result = import_result.copy()
                        success_result["status"] = "launched"
                        success_result["run_id"] = run.run_id
                        results.append(success_result)

                except Exception as e:
                    logger.warning(f"API     :  - An unexpected error occurred while ticking sensor '{sensor_name}'. Skipping.", exc_info=True)
                    import_result["status"] = "error"
                    import_result["error"] = str(e)
                    results.append(import_result)

        return jsonify({
            "message": f"Processed {len(selected_imports)} imports.",
            "results": results
        })
    except Exception as e:
        logger.error("API     : A critical failure occurred in the 'run_imports' endpoint.", exc_info=True)
        logger.error(
            "[RESOLUTION] This may be a problem with the Dagster instance, 'workspace.yaml', or the gRPC server. Run 'dagster dev' to check for code location loading errors."
        )
        return jsonify({"error": "An internal error occurred while ticking sensors."}), 500

@app.route("/api/run_status/<run_id>")
def get_run_status(run_id):
    """
    API endpoint to check the status of a specific Dagster run.
    """
    try:
        instance = DagsterInstance.get()
        run = instance.get_run_by_id(run_id)
        
        if not run:
            return jsonify({"error": "Run not found"}), 404
            
        response_data = {
            "run_id": run_id,
            "status": run.status.value
        }

        return jsonify(response_data)
    except Exception as e:
        logger.error(f"API     : Failed to fetch status for run '{run_id}'.", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/api/shutdown", methods=["POST"])
def shutdown():
    """
    API endpoint to shut down the application server.
    """
    # For a simple UI application served by Waitress and launched from a script,
    # a direct exit is the most reliable way to ensure the process terminates
    # when the user closes the application via the UI button.
    logger.info("--- Server Shutting Down ---")
    os._exit(0)

@app.errorhandler(404)
def not_found_error(error):
    """Custom 404 error handler to return JSON instead of HTML."""
    logger.warning(f"404 Not Found: {request.url}")
    return jsonify({"error": "Not Found", "message": f"The requested URL {request.path} was not found on the server."}), 404


@app.errorhandler(500)
def internal_error(error):
    """Custom 500 error handler to return JSON instead of HTML."""
    # This provides a clear, final error message for any unhandled exception.
    logger.error("--- Unhandled Internal Server Error ---", exc_info=error)
    logger.error("[RESOLUTION] A generic server error occurred. The traceback above contains the exact cause. Check for DB connection issues, errors in Dagster project code, or workspace configuration problems.")
    return jsonify({"error": "Internal Server Error", "message": "An unexpected error occurred on the server."}), 500

if __name__ == "__main__":
    # Use Waitress, a production-ready WSGI server.
    from waitress import serve

    # Start the initialization process in a separate thread.
    # This allows the web server to start immediately and serve the status page.
    init_thread = threading.Thread(target=_initialize_app_thread, daemon=True)
    init_thread.start()

    logger.info("Server  : Starting Data Importer UI on http://localhost:3000")
    logger.info(f"Server  : Logging detailed errors to {os.path.abspath('simple_ui.log')}")
    logger.info("Server  : Initialization is running in the background...")
    
    # The server starts immediately. The launcher script (`.bat` file) is responsible
    # for opening the web browser. The user will first see the status page.
    serve(app, host="0.0.0.0", port=3000)
