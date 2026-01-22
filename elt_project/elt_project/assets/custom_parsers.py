import pandas as pd
import os
import json
import time
import tempfile
import traceback

def parse_ri_dbt_custom(file_path: str) -> pd.DataFrame:
    """
    Custom parser for a file that has a dynamic header and footer.
    This is a good example of a true parsing task, as it deals with the
    physical structure of the file to correctly extract the tabular data.

    It assumes the file has:
    - Some number of initial rows to be ignored (header).
    - The actual CSV data.
    - A known marker indicating the end of the data (e.g., "END OF REPORT").
    """
    # --- Start of Custom Parsing Logic ---

    # This is where you would write the logic to handle the unique structure
    # of your file before it can be read into a DataFrame.

    # Example: Find how many rows to skip at the end of the file.
    # This is useful if a file has a summary footer.
    footer_skip = 0
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for i, line in reversed(list(enumerate(lines))):
            if "END OF REPORT" in line:
                footer_skip = len(lines) - i
                break

    # Use pandas read_csv with parameters derived from the file's structure.
    df = pd.read_csv(file_path, encoding='latin1', skiprows=2, skipfooter=footer_skip, engine='python')

    # --- End of Custom Parsing Logic ---
    return df

def parse_report_with_footer(file_path: str) -> pd.DataFrame:
    """
    Parses a CSV that has a 2-line header and a dynamic footer.
    """
    # Find how many rows are in the footer to skip them
    footer_skip = 0
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for i, line in reversed(list(enumerate(lines))):
            if "END OF REPORT" in line:
                footer_skip = len(lines) - i
                break

    # Use pandas with the dynamically discovered parameters
    df = pd.read_csv(file_path, encoding='latin1', skiprows=2, skipfooter=footer_skip, engine='python')
    return df

def generic_configurable_parser(file_path: str, parser_config_json: str) -> pd.DataFrame:
    """
    A generic, configuration-driven file parser.

    Instead of writing a new Python function, you define the parsing logic in a JSON
    object stored in the database. This function interprets that JSON to parse the file.

    Args:
        file_path (str): The path to the file to be parsed.
        parser_config_json (str): A JSON string containing the parsing configuration.

    Returns:
        pd.DataFrame: The parsed data.
    """
    config = json.loads(parser_config_json)
    
    # Get the pandas read options from the config, default to an empty dict.
    read_options = config.get("read_options", {})

    # --- Handle Pre-processing Steps (like dynamic footers) ---
    pre_processing = config.get("pre_processing", {})

    if "skip_footer_until_string" in pre_processing:
        # This logic is borrowed from the old custom parsers.
        footer_marker = pre_processing["skip_footer_until_string"]
        footer_skip = 0
        # Use the specified encoding if available, otherwise default to utf-8 for inspection.
        encoding_for_scan = read_options.get('encoding', 'utf-8')
        try:
            with open(file_path, 'r', encoding=encoding_for_scan) as f:
                lines = f.readlines()
                for i, line in reversed(list(enumerate(lines))):
                    if footer_marker in line:
                        footer_skip = len(lines) - i
                        break
            if footer_skip > 0:
                read_options["skipfooter"] = footer_skip
                read_options["engine"] = "python" # skipfooter requires the 'python' engine
        except UnicodeDecodeError:
            print(f"Warning: Could not scan file with encoding '{encoding_for_scan}' to find footer. Proceeding without skipfooter.")

    return pd.read_csv(file_path, **read_options)

def generic_selenium_scraper(scraper_config_json: str) -> dict[str, pd.DataFrame]:
    """
    A generic web scraper using Selenium, driven by a JSON configuration.
    This is passed the JSON config string directly, not a file path.

    Args:
        scraper_config_json: A string containing the JSON configuration for the scrape.

    Returns:
        A dictionary mapping target_import_name to its scraped pandas DataFrame.
    """
    def _log_error_to_simple_ui(msg):
        try:
            # Path to simple_ui.log relative to this file: ../../../simple_ui.log
            base_dir = os.path.dirname(os.path.abspath(__file__))
            log_path = os.path.abspath(os.path.join(base_dir, "..", "..", "..", "simple_ui.log"))
            
            # Try writing to simple_ui.log; fallback to scraper_error.log if locked
            target_file = log_path
            try:
                with open(target_file, "a") as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - generic_selenium_scraper - {msg}\n")
            except PermissionError:
                with open(os.path.join(os.path.dirname(log_path), "scraper_error.log"), "a") as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - ERROR - generic_selenium_scraper - {msg}\n")
        except Exception:
            pass

    try:
        config = json.loads(scraper_config_json)
    except json.JSONDecodeError as e:
        _log_error_to_simple_ui(f"JSON Decode Error: {e}")
        raise ValueError(f"Invalid JSON in scraper_config: {e}")

    # --- Lazy Import Selenium and related libraries ---
    # This ensures these packages are only imported when this function is actually called.
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.chrome.service import Service as ChromeService
    except ImportError as e:
        raise ImportError(f"Missing required scraping library: {e}. Please ensure 'selenium' and 'webdriver-manager' are installed.")

    # --- Helper function for data extraction ---
    def _extract_data(driver, extraction_config):
        # Navigate to a specific URL for this target if provided
        if "url" in extraction_config:
            driver.get(extraction_config["url"])

        if extraction_config["method"] == "html_table":
            table_index = extraction_config.get("table_index", 0)
            # Use pandas to read tables from the current page source
            tables = pd.read_html(driver.page_source)
            if len(tables) > table_index:
                return tables[table_index]
            else:
                # Return an empty DataFrame if the table is not found on a given page
                return pd.DataFrame()
        else:
            raise ValueError(f"Unsupported data extraction method: {extraction_config['method']}")

    # --- Helper function to check conditions ---
    def _check_condition(driver, condition_config):
        # For now, we only support 'element_exists'. This can be expanded.
        if condition_config["type"] == "element_exists":
            selector_map = {"id": By.ID, "name": By.NAME, "xpath": By.XPATH, "css_selector": By.CSS_SELECTOR, "link_text": By.LINK_TEXT, "class_name": By.CLASS_NAME}
            by = selector_map[condition_config["selector"]]
            try:
                # Use a short timeout to check for presence without a long wait
                WebDriverWait(driver, condition_config.get("timeout", 2)).until(
                    EC.presence_of_element_located((by, condition_config["selector_value"]))
                )
                return True
            except:
                return False
        raise ValueError(f"Unsupported condition type: {condition_config['type']}")

    # --- Recursive function to process a list of actions ---
    def _process_actions(driver, actions_list, scraped_data_accumulator):
        for action in actions_list:
            action_type = action["type"]
            selector_map = {
                "id": By.ID, "name": By.NAME, "xpath": By.XPATH,
                "css_selector": By.CSS_SELECTOR, "link_text": By.LINK_TEXT, "class_name": By.CLASS_NAME,
            }

            element = None
            # Pre-fetch element if a selector is present for standard actions
            if "selector" in action:
                by = selector_map[action["selector"]]
                wait = WebDriverWait(driver, action.get("timeout", 10))
                element = wait.until(EC.presence_of_element_located((by, action["selector_value"])))

            if action_type == "find_and_fill":
                value = os.getenv(action["value_env_var"])
                if value is None:
                    raise ValueError(f"Environment variable '{action['value_env_var']}' not set.")
                element.clear()
                element.send_keys(value)

            elif action_type == "find_and_fill_totp":
                import pyotp
                secret = os.getenv(action["totp_secret_env_var"])
                if secret is None:
                    raise ValueError(f"Environment variable '{action['totp_secret_env_var']}' for TOTP secret not set.")
                totp = pyotp.TOTP(secret)
                token = totp.now()
                element.clear()
                element.send_keys(token)

            elif action_type == "click":
                # Wait for element to be clickable before clicking
                wait.until(EC.element_to_be_clickable((by, action["selector_value"]))).click()

            elif action_type == "wait":
                time.sleep(action["duration_seconds"])

            elif action_type == "wait_for_element":
                # The element finding logic at the start of the loop already handles this.
                # This action type is useful for explicitly waiting for a page transition to complete.
                pass

            elif action_type == "if":
                if _check_condition(driver, action["condition"]):
                    _process_actions(driver, action.get("then", []), scraped_data_accumulator)
                else:
                    _process_actions(driver, action.get("else", []), scraped_data_accumulator)

            elif action_type == "while_loop":
                max_iterations = action.get("max_iterations", 10) # Safety break
                iterations = 0
                while iterations < max_iterations and _check_condition(driver, action["condition"]):
                    _process_actions(driver, action.get("loop_actions", []), scraped_data_accumulator)
                    iterations += 1

            elif action_type == "extract_and_accumulate":
                target_name = action.get("target_import_name")
                if not target_name:
                    raise ValueError("'extract_and_accumulate' action requires a 'target_import_name'.")
                
                new_df = _extract_data(driver, action)
                if not new_df.empty:
                    if target_name not in scraped_data_accumulator:
                        scraped_data_accumulator[target_name] = []
                    scraped_data_accumulator[target_name].append(new_df)

            else:
                raise ValueError(f"Unsupported Selenium action type: {action_type}")

    # --- 1. Setup Selenium WebDriver ---
    options = webdriver.ChromeOptions()
    is_headless = config.get("driver_options", {}).get("headless", False)
    if is_headless:
        options.add_argument("--headless")
        print("DEBUG: Running Selenium in HEADLESS mode.")
    else:
        print("DEBUG: Running Selenium in VISIBLE mode.")
    
    # NEW: Configure the browser's default download directory
    driver_options = config.get("driver_options", {})
    if "download_directory" in driver_options:
        prefs = {"download.default_directory": driver_options["download_directory"]}
        options.add_experimental_option("prefs", prefs)

    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    try:
        print("DEBUG: Initializing Selenium WebDriver...")
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        print("DEBUG: WebDriver initialized successfully.")
    except Exception as e:
        _log_error_to_simple_ui(f"WebDriver Init Failed: {e}\n{traceback.format_exc()}")
        traceback.print_exc()
        raise RuntimeError(f"Failed to initialize Selenium WebDriver. Ensure Google Chrome is installed. Error: {e}")

    scraped_data = {}
    scraped_data_accumulator = {} # For multi-page results
    try:
        # --- 2. Navigate to Login URL ---
        driver.get(config["login_url"])

        # --- 3. Execute Actions (now using the recursive processor) ---
        _process_actions(driver, config.get("actions", []), scraped_data_accumulator)

        # --- 4. Data Extraction ---
        # This now iterates over a list of extraction targets
        for extraction_target in config["data_extraction"]:
            target_name = extraction_target.get("target_import_name")
            if not target_name:
                raise ValueError("Each item in 'data_extraction' must have a 'target_import_name'.")
            df = _extract_data(driver, extraction_target)
            if not df.empty:
                # NEW: Save the extracted data to a file if 'output_file' is specified
                if "output_file" in extraction_target:
                    output_path = extraction_target["output_file"]
                    # Ensure directory exists
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    if output_path.lower().endswith('.xlsx'):
                        df.to_excel(output_path, index=False)
                    else:
                        df.to_csv(output_path, index=False)
                scraped_data[target_name] = df

    except Exception as e:
        _log_error_to_simple_ui(f"Scraping Runtime Error: {e}\n{traceback.format_exc()}")
        raise e

    finally:
        # --- 5. Cleanup ---
        driver.quit()

    # --- 6. Finalize Accumulated Data ---
    for target_name, df_list in scraped_data_accumulator.items():
        scraped_data[target_name] = pd.concat(df_list, ignore_index=True)

    return scraped_data

def generic_sftp_downloader(scraper_config_json: str) -> pd.DataFrame:
    """
    A generic SFTP downloader and parser, driven by a JSON configuration.

    This function connects to an SFTP server, downloads a specified file to a
    temporary local directory, parses it into a pandas DataFrame, and then cleans up.

    Args:
        scraper_config_json: A string containing the JSON configuration for the SFTP connection.

    Returns:
        A pandas DataFrame containing the data from the downloaded file.
    """
    # --- Lazy Import SFTP and related libraries ---
    try:
        import pysftp
    except ImportError:
        raise ImportError("The 'pysftp' library is required for SFTP downloads. Please install it via 'pip install pysftp'.")

    from .parsers import parser_factory # Use the existing parser factory
    import fnmatch

    try:
        config = json.loads(scraper_config_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in scraper_config: {e}")

    sftp_config = config.get("sftp_details")
    parse_config = config.get("parse_details")

    if not sftp_config or not parse_config:
        raise ValueError("SFTP config must contain 'sftp_details' and 'parse_details' sections.")

    # --- Get credentials securely from environment variables ---
    hostname = os.getenv(sftp_config["hostname_env_var"])
    username = os.getenv(sftp_config["username_env_var"])
    password = os.getenv(sftp_config.get("password_env_var")) # Password is optional for key-based auth
    private_key_path = os.getenv(sftp_config.get("private_key_path_env_var")) # For key-based auth

    if not all([hostname, username]):
        raise ValueError("SFTP hostname and username environment variables must be set.")

    # --- Production-Ready Host Key Handling ---
    # For development, you might disable host key checking.
    # For production, you should ALWAYS verify the host key.
    try:
        cnopts = pysftp.CnOpts()
    except Exception:
        # Fallback if known_hosts file is missing (common on Windows dev machines)
        cnopts = pysftp.CnOpts(knownhosts=None)
        cnopts.hostkeys = None

    if os.getenv("APP_ENV", "development").lower() == "production":
        # In production, load known host keys to prevent man-in-the-middle attacks.
        cnopts.hostkeys.load(os.path.expanduser("~/.ssh/known_hosts"))
    else:
        # In development, it's acceptable to disable host key checking for convenience.
        cnopts.hostkeys = None # WARNING: Disables host key checking.

    sftp_params = {
        "hostname": hostname,
        "username": username,
        "password": password,
        "private_key": private_key_path,
        "cnopts": cnopts
    }

    all_dfs = []

    # Create a temporary directory to download the file into
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Connecting to SFTP server at {hostname}...")
        try:
            with pysftp.Connection(**sftp_params) as sftp:
                remote_dir = sftp_config["remote_path"]
                file_pattern = sftp_config.get("file_pattern", "*") # Default to all files if no pattern

                print(f"Listing files in remote directory '{remote_dir}' matching pattern '{file_pattern}'...")
                
                # List files and filter by pattern
                remote_files = sftp.listdir(remote_dir)
                matching_files = [f for f in remote_files if fnmatch.fnmatch(f, file_pattern)]

                if not matching_files:
                    print("No matching files found on SFTP server. Returning empty DataFrame.")
                    return pd.DataFrame()

                print(f"Found {len(matching_files)} matching files to download.")

                # Download and parse each matching file
                for filename in matching_files:
                    remote_filepath = f"{remote_dir}/{filename}"
                    local_filepath = os.path.join(temp_dir, filename)
                    
                    print(f"Downloading '{remote_filepath}' to '{local_filepath}'...")
                    sftp.get(remote_filepath, local_filepath)

                    # Use the existing parser factory to parse the downloaded file
                    parser = parser_factory.get_parser(parse_config["file_type"])
                    df_single = parser.parse(local_filepath)
                    all_dfs.append(df_single)
                    print(f"Successfully parsed {len(df_single)} rows from '{filename}'.")
        except Exception as e:
            raise RuntimeError(f"SFTP Operation Failed: {e}") from e

    # Concatenate all DataFrames into one
    if not all_dfs:
        return pd.DataFrame()

    final_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Successfully concatenated all files. Total rows: {len(final_df)}")
    return final_df




# Add more custom parser functions here as needed.