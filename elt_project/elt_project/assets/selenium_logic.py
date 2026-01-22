import json
import os
import time
import pandas as pd

def generic_selenium_scraper(config_json: str):
    """
    Executes a Selenium-based scraping workflow defined in JSON.
    Supports TOTP generation for automated 2FA.
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service as ChromeService
    from webdriver_manager.chrome import ChromeDriverManager

    config = json.loads(config_json)
    
    # --- 1. Setup Driver ---
    options = webdriver.ChromeOptions()
    # Default to headless unless explicitly disabled in config
    if config.get("driver_options", {}).get("headless", True):
        options.add_argument("--headless")
    
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    
    # Automatically manage ChromeDriver installation
    driver_path = ChromeDriverManager().install()
    # WinError 193 fix: Ensure path ends in .exe
    if not str(driver_path).lower().endswith(".exe"):
        directory = os.path.dirname(driver_path) if os.path.isfile(driver_path) else driver_path
        potential_exe = os.path.join(directory, "chromedriver.exe")
        if os.path.exists(potential_exe):
            driver_path = potential_exe
            
    driver = webdriver.Chrome(service=ChromeService(driver_path), options=options)
    
    try:
        # --- 2. Login / Navigation ---
        if "login_url" in config:
            driver.get(config["login_url"])
        
        # Execute sequence of actions (fill forms, click buttons, wait, handle TOTP)
        for action in config.get("actions", []):
            _perform_action(driver, action)
            
        # --- 3. Data Extraction ---
        results = {}
        for extraction in config.get("data_extraction", []):
            df = _perform_extraction(driver, extraction)
            
            # Save to file if 'output_file' is specified in config
            if "output_file" in extraction and not df.empty:
                os.makedirs(os.path.dirname(extraction["output_file"]), exist_ok=True)
                df.to_csv(extraction["output_file"], index=False)

            target_name = extraction.get("target_import_name", "default")
            results[target_name] = df
            
        # Return single DataFrame if only one target, else return the dictionary
        if len(results) == 1:
            return list(results.values())[0]
        return results
        
    finally:
        driver.quit()

def _perform_action(driver, action):
    import pyotp

    action_type = action["type"]
    
    if action_type == "wait":
        time.sleep(action.get("duration_seconds", 1))
        
    elif action_type == "find_and_fill":
        elem = _find_element(driver, action)
        val = os.getenv(action["value_env_var"])
        if val is None:
            raise ValueError(f"Environment variable '{action['value_env_var']}' is missing.")
        elem.clear()
        elem.send_keys(val)
        
    elif action_type == "find_and_fill_totp":
        elem = _find_element(driver, action)
        secret = os.getenv(action["totp_secret_env_var"])
        if not secret:
            raise ValueError(f"TOTP secret env var '{action['totp_secret_env_var']}' not set.")
        # Generate the current TOTP code
        totp = pyotp.TOTP(secret)
        code = totp.now()
        elem.clear()
        elem.send_keys(code)
        
    elif action_type == "click":
        elem = _find_element(driver, action)
        elem.click()
        
    elif action_type == "wait_for_element":
        timeout = action.get("timeout", 10)
        _find_element(driver, action, timeout=timeout)

def _find_element(driver, action, timeout=10):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    by_map = {
        "id": By.ID, "css_selector": By.CSS_SELECTOR, "xpath": By.XPATH,
        "name": By.NAME, "class_name": By.CLASS_NAME
    }
    by = by_map.get(action.get("selector", "id"), By.ID)
    val = action["selector_value"]
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, val)))

def _perform_extraction(driver, extraction):
    if extraction["method"] == "html_table":
        # Requires lxml or beautifulsoup4 and html5lib installed
        dfs = pd.read_html(driver.page_source)
        idx = extraction.get("table_index", 0)
        return dfs[idx]
    return pd.DataFrame()