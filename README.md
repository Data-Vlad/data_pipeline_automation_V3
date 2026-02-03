# Data Pipeline Automation & Analytics Hub

**A robust, metadata-driven ELT framework integrated with a modern AI Analytics Hub.**

## Table of Contents

1.  Overview
2.  System Architecture
3.  Key Features
4.  Installation & Setup
5.  Running the Application
6.  Hosting & Remote Access (The Tunnel)
7.  User Guide: Data Importer
8.  User Guide: Analytics & AI Hub
9.  Developer Guide
10. Security & Governance

---

## Overview

This project automates the ingestion of data from various sources (CSV, Excel, Web Scrapers, SFTP) into a centralized SQL Server data warehouse. It pairs this robust ELT framework with a state-of-the-art **Analytics & AI Hub** that provides predictive insights, natural language querying, and automated data storytelling.

The system is designed to be "Low Code" for operations but "High Code" for capabilities. Pipelines are defined via metadata in a database, while the logic is handled by reusable Python assets and SQL stored procedures.

---

## System Architecture

The solution consists of four main components running in concert:

1.  **Data Importer UI (Flask)**: A lightweight web interface (`simple_ui.py`) running on port `3000`. It serves as the entry point, handling user login and allowing users to trigger data imports manually.
2.  **Analytics & AI Hub (Streamlit)**: A powerful dashboard (`analytics_ui.py`) running on port `8501`. It connects directly to the data warehouse to provide real-time insights, AI forecasting, and data exploration.
3.  **Orchestration Engine (Dagster)**: Manages the execution of data pipelines, dependencies, sensors, and logging.
4.  **Data Warehouse (SQL Server)**: Stores all configuration, raw data (Staging), transformed data (Destination), and logs.

---

## Key Features

### ELT & Data Engineering
*   **Metadata-Driven**: New pipelines are created by adding a row to a SQL table (`elt_pipeline_configs`). No new Python code is required for standard files.
*   **Smart Batching**: Automatically detects batch file drops and switches from `REPLACE` to `APPEND` mode to prevent data loss.
*   **Automated Enrichment**: Fills in missing data (e.g., looking up `Category` from `SKU`) before loading to staging.
*   **Data Governance**: Defines quality rules (e.g., "Sales cannot be negative") in the database. Pipelines halt immediately if critical rules fail.

### Analytics & AI
*   **Conversational Analytics**: Ask questions in plain English ("Why did sales drop?") and get SQL-generated answers.
*   **Predictive Insights**: Automated forecasting and anomaly detection using Machine Learning.
*   **Semantic Search**: Search your data by meaning (RAG), not just keywords.
*   **Auto-Dashboards**: The AI analyzes your table schema and automatically generates the best visualization.

---

## Installation & Setup

### 1. Prerequisites
*   **OS**: Windows 10/11 or Server.
*   **Python**: Version 3.8 or higher. Download Here.
    *   *Important*: Check **"Add Python to PATH"** during installation.
*   **Git**: Download Here.
*   **SQL Server**: A running instance (Local or Azure SQL) and the **ODBC Driver 17 for SQL Server**.

### 2. Database Setup
1.  Create a new database (e.g., `DataPipelineDB`).
2.  Execute the SQL scripts in `elt_project/sql/` in order:
    *   `01_setup_log_table.sql`
    *   `02_setup_data_governance.sql`
    *   `03_manage_elt_pipeline_configs.sql`
    *   `05_setup_data_enrichment.sql`

### 3. Environment Configuration
Create a `.env` file in the project root:

```ini
DB_SERVER=localhost
DB_DATABASE=DataPipelineDB
DB_DRIVER={ODBC Driver 17 for SQL Server}
CREDENTIAL_TARGET=DataLaunchpad/DB_Credentials
OPENAI_API_KEY=sk-... (Optional: For AI features)
ANALYTICS_PUBLIC_URL=http://localhost:8501
```

### Step 3: Database Setup

1.  Connect to your SQL Server instance using a tool like SSMS or Azure Data Studio.
2.  Create a new database for this project (e.g., `dagster_elt_framework`).
3.  Execute the SQL scripts located in the `elt_project/sql/` directory in the correct order to create the necessary tables and procedures:
    1.  `01_setup_log_table.sql`
    2.  `02_setup_data_governance.sql` (Creates data quality tables)
    3.  `03_manage_elt_pipeline_configs.sql` (Creates the main config table)
    4.  `05_setup_data_enrichment.sql` (Creates enrichment rules table)
    5.  *(Execute any other custom table/procedure scripts you have)*

> **Note**: The SQL files in the `sql/` directory are numbered to suggest an execution order.
### Step 4: Environment Configuration

Create a `.env` file in the root of the project directory. This file securely stores your database credentials. Copy the template below and fill in your details.

```ini
# .env

DB_DRIVER="{ODBC Driver 17 for SQL Server}"
DB_SERVER="your_server_name_or_ip"
DB_DATABASE="dagster_elt_framework"
DB_USERNAME="your_username"
DB_PASSWORD="your_password"
DB_TRUST_SERVER_CERTIFICATE="yes"
```

### 3.5. Run and Verify

1.  **Start Dagster**: Open a terminal in the project root and run:
    ```bash
    dagster dev
    ```
2.  **Open the UI**: Navigate to `http://localhost:3000` in your web browser. You should see the assets and jobs that have been dynamically generated.

## 4. How to Add a New Pipeline (The Easy Way)

This framework uses utility assets to automate the most tedious parts of creating a new pipeline: writing DDL and configuring column mappings.

### Step 1: Configure the Pipeline in the Database

First, add a new row to your `elt_pipeline_configs` table. This record defines your new pipeline. At a minimum, you need to specify names, file patterns, and table names.

**Example SQL:**
```sql
INSERT INTO elt_pipeline_configs (
    pipeline_name, import_name, file_pattern, monitored_directory, file_type,
    staging_table, destination_table, transform_procedure, load_method, is_active
) VALUES (
    'NewReports', 'new_daily_report', 'NewReport-*.csv', 'C:\\data\\incoming\\new_reports', 'csv',
    'stg_new_daily_report', 'dest_new_daily_report', 'sp_transform_new_reports', 'replace', 1
);
```

### Step 2: Generate the DDL (SQL)

1.  **Place a Sample File**: Put a sample source file (e.g., `NewReport-01.csv`) in the `monitored_directory` you defined (`C:/data/incoming/new_reports`).
2.  **Reload & Run**: Restart `dagster dev` to load your new configuration. In the UI, go to the `_utility` asset group and materialize the `newreports_generate_setup_sql` asset.
3.  **Execute the SQL**: The asset run will succeed. Click on the asset in the UI, go to the "Asset-level metadata" tab, and find the `generated_sql_setup` entry. Copy the `CREATE TABLE` and `CREATE PROCEDURE` scripts and execute them in your database.

### Step 3: Generate the Column Mappings

1.  **Run the Mapping Asset**: Now that the tables exist, go back to the `_utility` group in the UI and materialize the `newreports_generate_column_mappings` asset.
2.  **Done**: This asset will inspect your source file and the newly created staging table, generate the correct positional column mapping, and automatically save it to the database for you.

Your new pipeline is now fully configured and ready to process files.

---

## User Guide: Running Pipelines with the Simple UI
## Credential Management: Using Windows Credential Manager
## Configuration Guide

This guide explains how to use the simple, user-friendly interface to run data imports. With just one click, it handles all the technical setup for you, allowing you to select and run pipelines from a clean web page.
To connect to the database securely without asking for your password every time, the service uses the built-in Windows Credential Manager. This is a secure, one-time setup.
All configuration is managed in the `elt_pipeline_configs` SQL table.

### Step 1: Prerequisite (One-Time Check)
If the runner script cannot find the credentials it needs, it will display a warning and instructions. Follow the steps below to add them.
| Column | Description |
| :--- | :--- |
| `pipeline_name` | Grouping name for related imports (e.g., "Finance_Reports"). |
| `import_name` | Unique identifier for this specific import (e.g., "fin_budget_2024"). |
| `file_pattern` | Glob pattern to match files (e.g., `Budget_*.csv`). |
| `monitored_directory` | Full path to the folder where files are dropped. |
| `staging_table` | Name of the SQL table for raw data load. |
| `destination_table` | Name of the final SQL table. |
| `transform_procedure` | Stored procedure to merge Staging -> Destination. |
| `load_method` | `REPLACE` (truncate then load) or `APPEND` (add to existing). |
| `depends_on` | (Optional) Comma-separated list of `import_name`s that must run first. |

The only program you need on your computer is **Python**. Here’s how to check if you have it:
### Step-by-Step Guide to Add Credentials
### Setting up Dependencies
To ensure `Import_B` runs after `Import_A`:
1.  Configure `Import_A` normally.
2.  In `Import_B`'s config, set `depends_on` to `'Import_A'`.
3.  **Restart Dagster** to apply the new dependency graph.

1.  Open the Windows Start Menu and type `cmd`.
2.  Select **Command Prompt**.
3.  In the black window that appears, type `python --version` and press Enter.
1.  Open the **Start Menu** and type "**Credential Manager**", then open it.

*   **If you see a version number** (e.g., `Python 3.10.4`), you are all set! You can close the Command Prompt window.
*   **If you see an error message** like "'python' is not recognized...", you will need to install it.
    

#### How to Install Python:
2.  Select **Windows Credentials**.

1.  Go to the official Python website: python.org/downloads/
2.  Click the "Download Python" button to get the latest version.
3.  Run the installer. **IMPORTANT**: On the first screen of the installer, make sure to check the box at the bottom that says **"Add Python to PATH"**.
    !Add Python to PATH
4.  Click "Install Now" and follow the prompts to complete the installation.
    

### Step 2: Running the Service
3.  Click on **Add a generic credential**.

You will be provided with a single file: `run_elt_service.bat`.
    

1.  Save this file anywhere on your computer, for example, on your Desktop.
2.  **Simply double-click the `run_elt_service.bat` file to start everything.**
4.  Fill in the three fields exactly as described below:

### Step 4: What to Expect During Startup
    -   **Internet or network address:**
        ```
        DagsterELTFramework/DB_Credentials
        ```
        *(This must match exactly what the script is looking for.)*

When you double-click the script, a few things will happen automatically. The first time you run it, it may take a few minutes to download and set up everything. Subsequent startups will be much faster.
    -   **User name:**
        Your database username (e.g., `sql_user_name`).

1.  **One-Time Credential Setup**: The very first time you run the script, it will prompt you to enter your database username and password. It will save these securely in the Windows Credential Manager so you will **never be asked for them again**.
 > **Note for Administrators**: For easier management across different offices or teams, it is recommended to create a dedicated, read-only SQL Server login for each group. Users in that office can then use this shared, read-only credential for the one-time setup. This simplifies access control and auditing.
1.  **A black window will appear**, showing setup progress with messages like `[1/8] Checking for Python...`.
2.  The script will automatically download the latest version of the pipeline code and install any necessary software components.
3.  After the setup is complete, a single command prompt window will remain open. This window runs the background services. You can minimize this window, but do not close it.
4.  Finally, your default **web browser will automatically open** to the **ELT Pipeline Runner** interface (`http://localhost:3000`).

You are now ready to run your imports.

### Step 5: How to Run Imports

The web page provides a simple, checklist-based interface for running your data imports.

1.  **Select Imports**: The page displays all available pipelines, grouped together. Check the box next to each specific import you wish to run.
2.  **Start the Run**: Once you have selected at least one import, the **"Run Selected Imports"** button at the bottom will become active. Click it to begin.
3.  **Monitor the Status**: After clicking the button, a **"Run Status"** section will appear on the page.
    *   Each pipeline run you initiated will be listed here.
    *   The status will update in real-time, from `STARTING` to `SUCCESS` or `FAILURE`.
    *   A spinning loader indicates that a run is still in progress.
    *   For troubleshooting, you can click the `(details)` link next to any run to open the detailed technical logs in a new tab.
    *   **Queue Processing**: If a specific import fails or does not exist, the system will automatically move on to load the next import in the queue.

### Step 6: How to Stop the Service

When you are finished, simply **close the command prompt window** that opened when you started the service. This will safely shut down all background processes. You can also close the browser tab at any time.

### Common Questions & Troubleshooting
*   **"How do I know if my file was processed?"**
    *   The **Run Status** section on the web page is the primary source of feedback. It will clearly show whether your selected imports succeeded or failed.
*   **"The script shows an error and closes immediately."**
    *   This usually means the Python prerequisite from Step 1 is not met. Please double-check that Python is installed and that the "Add Python to PATH" box was checked during installation.
*   **"The script says 'Cannot run in offline mode...'"**
    *   The very first time you run the script, you need an internet connection so it can download the pipeline code. Please connect to the internet and try again. After the first run, it will be able to start up offline.
*   **"The script asks for my credentials every time."**
    *   This can happen if your system permissions prevent the script from saving to the Windows Credential Manager. Please contact your IT support for assistance.
*   **"One of the Dagster windows has red text and errors."**
    *   This indicates a problem with the underlying service. You can close the window and try running the `run_elt_service.bat` script again. If the problem persists, please take a screenshot of the error message and contact the support team. You can also click the `(details)` link in the UI for more information.

---

## Administrator Guide
## Utility Assets

This guide is for advanced users and administrators responsible for maintaining the ELT framework. Administration tasks are intentionally separated from the simple end-user interface and are performed using standard developer tools.
The framework includes "Utility" assets to help you set up new pipelines quickly. These can be triggered manually from the Dagster UI (Group: `_utility`).

### Managing Configurations (Pipelines & Rules)
*   **`generate_ddl`**: Reads a sample file from the monitored directory and generates the `CREATE TABLE` and `CREATE PROCEDURE` SQL scripts needed for the pipeline.
*   **`generate_column_mapping`**: Auto-generates the JSON mapping between your CSV headers and SQL columns if they don't match perfectly.
*   **`generate_setup_sql`**: Generates a consolidated SQL script for an entire pipeline group.
*   **`backup_database_objects`**: Scripts out your configuration tables and stored procedures to a local `backups/` folder for disaster recovery.

The definitions for all pipelines, imports, and data quality rules are stored as metadata in the SQL database. The most direct and powerful way to manage these configurations is through a dedicated SQL client.

*   **Tool**: Use a standard SQL client like **SQL Server Management Studio (SSMS)** or **Azure Data Studio**.
*   **What you can do**:
    *   Perform CRUD (Create, Read, Update, Delete) operations on pipeline definitions in the `elt_pipeline_configs` table.
    *   Manage data validation rules in the `data_quality_rules` table.
*   **Why**: This approach provides precise control and is the standard practice for database administration.

### Running Utilities & Maintenance Jobs

Administrative actions, such as generating SQL for a new pipeline, creating database backups, or running column mapping utilities, are performed using the full Dagster developer interface.

*   **How to Access**:
    1.  Open a command prompt (like `cmd` or PowerShell).
    2.  Navigate to the project directory.
    3.  Run the command: `run_elt_service.bat --dev`
*   **What you can do**:
    *   Materialize `_utility` assets to generate DDL and column mappings for new pipelines.
    *   Run the `utility_backup_database_objects` asset.
    *   Get a detailed, technical view of all pipeline runs, logs, and asset health.
*   **Why**: The full Dagster UI is purpose-built for these operational tasks and provides the detailed logging and visual feedback necessary for development and maintenance.

### Summary of Admin Roles

| Task                                       | Tool to Use                               | How to Access                               |
| ------------------------------------------ | ----------------------------------------- | ------------------------------------------- |
| **Manage Pipeline/Rule Definitions**       | SQL Client (SSMS, Azure Data Studio)      | Connect directly to the SQL database.       |
| **Run Utility & Maintenance Jobs**         | Dagster Developer UI                      | Run `run_elt_service.bat --dev`.            |

---

## 5. End-to-End Workflow

This section describes the complete journey of data from ingestion to its final destination.

### 1. Data Ingestion (The Trigger)

The process begins when data becomes available.

*   **Method A: File-Based (Sensor-Driven)**
    *   **Action**: A user or an automated process drops a file (e.g., `daily_sales.csv`) into a folder specified in the `monitored_directory` configuration.
    *   **Result**: A Dagster sensor, which is actively polling that directory, detects the new file. It automatically triggers a new pipeline run, passing the specific file path to the execution context.

*   **Method B: SFTP Download (Manual or Scheduled Run)**
    *   **Action**: A user manually materializes the SFTP asset (e.g., `sftp_daily_sales_extract_and_load_staging`) in the Dagster UI, or a pre-defined schedule triggers it.
    *   **Result**: The `generic_sftp_downloader` function executes, connects to the SFTP server, downloads the matching file(s), and proceeds.

*   **Method C: Web Scraping (Manual or Scheduled Run)**
    *   **Action**: A user manually materializes the web scraping asset (e.g., `complex_site_data_extract_and_load_staging`) or a schedule triggers it.
    *   **Result**: The `generic_selenium_scraper` function executes, launches a browser, follows the actions defined in its `scraper_config`, and extracts the data.

### 2. Extract, Parse, and Load to Staging

The `[import_name]_extract_and_load_staging` asset is now active. It performs the following actions:
1.  **Parses Data**: It reads the source data (from the file path or the scraper's output) and parses it into a pandas DataFrame using the appropriate parser (e.g., `csv`, `excel`, or a custom function).
2.  **Applies Mapping**: If a `column_mapping` is defined, it renames the DataFrame's columns to match the staging table's schema.
3.  **Adds Lineage**: It adds a `dagster_run_id` column to every row, tagging the data with a unique identifier for this specific run.
4.  **Loads to Staging**: It loads the processed DataFrame into the SQL `staging_table` defined in the configuration.

### 3. Data Quality Validation

Immediately after the data is loaded into the staging table, the same asset executes the `sp_execute_data_quality_checks` stored procedure.
*   It validates the newly loaded data (filtered by the current `dagster_run_id`) against all active rules in the `data_quality_rules` table.
*   If any rule with `severity = 'FAIL'` fails, the asset run is halted, an error is raised, and the entire pipeline stops to prevent bad data from propagating.

### 4. Transform and Load to Destination

If the staging and validation steps succeed, the downstream `[pipeline_group_name]_transform` asset is executed.
1.  **Deduplication (Optional)**: If the pipeline's `load_method` is `append` and a `deduplication_key` is provided, it first removes any rows from the staging table that already exist in the destination table based on that key.
2.  **Executes Transformation**: It calls the `transform_procedure` (a SQL stored procedure) specified in the configuration. It passes two key parameters: the `dagster_run_id` and a list of tables to truncate (if the `load_method` is `replace`).
3.  **Loads to Destination**: The stored procedure contains the business logic to clean, reshape, and `INSERT` the data from the staging table into the final `destination_table`.

## 6. Ingestion Methods Explained

---

### Automated Data Enrichment

A common challenge in data pipelines is handling incomplete source data. For example, a sales transaction file might contain a `product_sku` but be missing the `product_category`. This framework includes a metadata-driven enrichment feature that can automatically fill in these gaps by looking up values from other database tables.

**How it works:**

The enrichment logic is executed within the `extract_and_load_staging` asset, after the source file has been parsed into a DataFrame but *before* it is loaded into the staging table.

1.  **Define Rules as Data**: You define enrichment rules in the `data_enrichment_rules` table. Each rule specifies the staging table and column to enrich, the "lookup" table that contains the missing information, and the key columns to join them on.
2.  **Automated Lookup**: For each active rule matching the current staging table, the framework performs a lookup. It reads the lookup table (e.g., `dim_products`), joins it to the source data in memory (on `product_sku`), and uses the result to fill in `NULL` values in the target column (`product_category`).
3.  **Load Enriched Data**: The now-enriched DataFrame is loaded into the staging table.

This approach allows you to add powerful data enrichment capabilities without writing any new Python or SQL code.

#### Example: Enriching Sales Data with Product Category

**Scenario**: You have a `stg_sales` table that is loaded from a CSV. The source CSV has `product_sku` but often has a `NULL` `product_category`. You also have a `dim_products` dimension table that maps every `sku` to its `category`.

**Goal**: Automatically fill in the `product_category` in `stg_sales` before the data is staged.

**Step 1: Create the Dimension Table**

First, ensure your lookup table exists and is populated.

```sql
-- Your master product dimension table
CREATE TABLE dim_products (
    sku NVARCHAR(50) PRIMARY KEY,
    product_name NVARCHAR(255),
    category NVARCHAR(100)
);

INSERT INTO dim_products (sku, product_name, category) VALUES
('ABC-123', 'Widget A', 'Electronics'),
('XYZ-789', 'Widget B', 'Home Goods');
```

**Step 2: Add an Enrichment Rule**

Insert a new rule into the `data_enrichment_rules` table.

```sql
INSERT INTO data_enrichment_rules (
    rule_name,
    description,
    target_staging_table,       -- The table we are enriching
    target_column_to_enrich,    -- The column we want to fill
    lookup_table,               -- The table that has the data
    lookup_key_column_staging,  -- The key in our source data
    lookup_key_column_lookup,   -- The corresponding key in the lookup table
    lookup_value_column         -- The column in the lookup table with the value we want
) VALUES (
    'enrich_sales_product_category_from_sku',
    'When stg_sales.product_category is NULL, find it in dim_products using the SKU.',
    'stg_sales', 'product_category', 'dim_products', 'product_sku', 'sku', 'category'
);
```

**Step 3: Run the Pipeline**

That's it! The next time your pipeline for `stg_sales` runs, the framework will automatically detect and apply this rule. Any row in your source file with a `product_sku` but a missing `product_category` will have the category correctly filled in from `dim_products` before being inserted into the staging table.

---

The ingestion method is determined by the `parser_function` and `scraper_config` columns in the `elt_pipeline_configs` table.

### Method 1: Local File System (Default)

This is the most common method. The framework monitors a local directory for new or modified files that match a specific pattern.

*   **How it works**: A Dagster sensor polls the `monitored_directory`. When a file matching `file_pattern` is found, a pipeline run is triggered. The `extract_and_load_staging` asset reads this file, parses it based on the `file_type` (e.g., `csv`, `excel`), and loads it into the staging table.
*   **When to use**: When your source data is delivered as files (e.g., CSV, PSV, Excel) to a folder on the same machine or a network share accessible by the Dagster instance.

#### Implementation Steps:
1.  Set `parser_function` to `NULL`.
2.  Define file details in `elt_pipeline_configs`:
    *   `monitored_directory`: The absolute path to the folder to watch. **Use forward slashes** (e.g., `'C:/Pipelines/Incoming'`).
    *   `file_pattern`: The pattern to match files (e.g., `'daily_sales_*.csv'`).
    *   `file_type`: The type of file to parse (e.g., `'csv'`, `'excel'`, `'psv'`).
3.  **Enable the Sensor**: In the Dagster UI, go to the **Sensors** tab and turn on the dynamically generated sensor for your pipeline.

### Method 2: SFTP Server Download

This method allows the framework to connect to an SFTP server, download one or more files matching a pattern, and then process them.

*   **How it works**: When the asset is materialized (manually or on a schedule), it uses the `generic_sftp_downloader` function. This function reads its connection and file details from a JSON object in the `scraper_config` column, connects to the SFTP server, downloads the matching files to a temporary location, and then parses them.
*   **When to use**: When source files are located on a remote SFTP server.

#### Implementation Steps:
1.  Set `parser_function` to `'generic_sftp_downloader'`.
2.  Store SFTP credentials in your `.env` file.
3.  Populate the `scraper_config` column with a JSON object specifying connection details and the file(s) to download.

**Example `scraper_config`:**
```json
{
    "sftp_details": {
        "hostname_env_var": "MY_SFTP_HOST",
        "username_env_var": "MY_SFTP_USER",
        "password_env_var": "MY_SFTP_PASSWORD",
        "remote_path": "/remote/data/outgoing/",
        "file_pattern": "report-*.csv"
    },
    "parse_details": {
        "file_type": "csv"
    }
}
```

### Method 3: Web Scraping with Selenium

For modern, dynamic websites that rely on JavaScript to render content, this method uses a full browser (controlled by Selenium) to navigate, log in, and extract data.

*   **How it works**: When materialized, the asset invokes the `generic_selenium_scraper` function. This function reads a sequence of actions (e.g., "find and fill", "click", "wait") from the `scraper_config` JSON. It executes these actions in a real browser to simulate a user, then extracts data from the resulting page (e.g., from an HTML table).
*   **When to use**: For websites where a simple HTTP request is not enough because the data is loaded dynamically with JavaScript, or for sites with complex, multi-step login forms (including TOTP).

#### Implementation Steps:
1.  **Set `parser_function` to `'generic_selenium_scraper'`**.
2.  **Store credentials in `.env`**: Add username, password, and TOTP secrets to your `.env` file.
3.  **Create the JSON configuration** for the `scraper_config` column. This JSON is a script of actions for the browser to follow. You can test this configuration locally using the `test_scraper_config.py` utility script before deploying.

**Example `scraper_config` JSON:**
```json
{
    "login_url": "https://some-complex-site.com/login",
    "actions": [
        { "type": "find_and_fill", "selector": "id", "selector_value": "username", "value_env_var": "COMPLEX_SITE_USERNAME" },
        { "type": "click", "selector": "xpath", "selector_value": "//button[text()='Sign In']" },
        { "type": "wait_for_element", "selector": "id", "selector_value": "dashboard", "timeout": 15 }
    ],
    "data_extraction": [
        { "target_import_name": "dashboard_data", "method": "html_table", "table_index": 0 }
    ]
}
```

### Method 4: Custom Python Parser

For unique file formats that don't fit the standard parsers, you can write your own Python parsing function.

*   **How it works**: You create a Python function in `elt_project/core/custom_parsers.py` that takes a `file_path` and returns a pandas DataFrame. You then specify the name of this function in the `parser_function` column. The framework will call your function directly instead of using the generic parser factory.
*   **When to use**: For proprietary file formats, files with complex header/footer logic, or any source that requires bespoke Python code to be read correctly.

#### Implementation Steps:
1.  **Write the function** in `elt_project/core/custom_parsers.py`.
2.  **Whitelist the function name** in the `ALLOWED_CUSTOM_PARSERS` set in `elt_project/assets/factory.py` for security.
3.  **Set `parser_function`** in the database to the exact name of your Python function (e.g., `'parse_report_with_footer'`).

##### Detailed Example

Let's walk through a complete example. Imagine you have a CSV file that has a 2-line header and a dynamic footer that always starts with "END OF REPORT". Standard parsers can't handle this, so we'll create a custom one.

##### Step 1: Write Your Custom Parser Function

Open `elt_project/core/custom_parsers.py` and add your function. It must accept a `file_path` string and return a pandas DataFrame.

**`elt_project/core/custom_parsers.py`**
```python
import pandas as pd

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

# Add other custom parsers here...
```

##### Step 2: Whitelist Your Function for Security

To prevent arbitrary code execution, you must explicitly approve your new function. Open `elt_project/assets/factory.py` and add your function's name to the `ALLOWED_CUSTOM_PARSERS` set.

**`elt_project/assets/factory.py`**
```python
    # ... inside create_extract_and_load_asset ...
    ALLOWED_CUSTOM_PARSERS = {
        "parse_ri_dbt_custom",
        "parse_report_with_footer", # <-- Add your function name here
        "generic_web_scraper",
        # ... other allowed functions
    }
```

##### Step 3: Configure the Pipeline in the Database

Now, tell the framework to use your new function for a specific pipeline by setting the `parser_function` column in the `elt_pipeline_configs` table.

```sql
-- Example: Configure the 'special_report' pipeline to use your custom parser
UPDATE elt_pipeline_configs
SET
    parser_function = 'parse_report_with_footer',
    file_type = 'custom' -- It's good practice to set a descriptive file_type
WHERE import_name = 'my_special_report';
```

### Method 5: Generic Configurable Parser (JSON-driven)

This method offers a highly flexible way to parse files by defining the parsing logic directly in a JSON configuration stored in the database. It leverages a single, generic Python function (`generic_configurable_parser`) that interprets this JSON to dynamically configure how `pandas.read_csv` should read your file.

*   **How it works**: You specify `'generic_configurable_parser'` in the `parser_function` column and provide a JSON object in the `scraper_config` column. This JSON can define standard `pandas.read_csv` parameters (like `skiprows`, `delimiter`, `encoding`) and even pre-processing steps (like dynamically skipping footer rows based on a marker string).
*   **When to use**: For files that are fundamentally CSV-like but require specific, non-standard `read_csv` parameters, or dynamic pre-processing (e.g., variable footers). It allows you to handle many bespoke file formats without writing a new Python function for each one.

#### Implementation Steps

1.  **Set `parser_function` to `'generic_configurable_parser'`**.
2.  **Populate `scraper_config`** with your JSON parsing configuration.

**Example `scraper_config` JSON:**
This example configures the parser to skip the first 3 rows, use a semicolon as a delimiter, and dynamically skip footer rows until "END OF DATA" is found.
```json
{
    "read_options": {
        "skiprows": 3,
        "delimiter": ";",
        "encoding": "latin1"
    },
    "pre_processing": {
        "skip_footer_until_string": "END OF DATA"
    }
}
```

**Example SQL `INSERT` statement:**
```sql
INSERT INTO elt_pipeline_configs (
    pipeline_name, import_name, file_pattern, monitored_directory, file_type,
    staging_table, destination_table, transform_procedure,
    parser_function, scraper_config -- Set these columns
) VALUES (
    'ConfigurableReports', 'configurable_csv', 'report_*.txt', 'C:\\data\\configurable', 'custom_csv',
    'stg_configurable_report', 'dest_configurable_report', 'sp_transform_configurable_report',
    'generic_configurable_parser', '{"read_options": {"skiprows": 3, "delimiter": ";"}, "pre_processing": {"skip_footer_until_string": "END OF DATA"}}'
);
```

### Load Methods (`replace` vs. `append`)

The `load_method` column in the `elt_pipeline_configs` table controls how data is written to the final **destination table**. This logic is handled atomically within the transformation stored procedure, ensuring data integrity.

1.  **Staging Behavior (Universal)**: For every run, data from the source file is always **appended** to the staging table. Each new batch of data is tagged with a unique `dagster_run_id`.

2.  **Destination Behavior (Configurable)**: The `transform` asset reads the `load_method` from the configuration and instructs the stored procedure accordingly.

*   **`replace`** (Default):
    *   **Sensor-Triggered Run (Single File)**: When a sensor detects a single file for an import configured with `replace`, the `transform` asset instructs the stored procedure to `TRUNCATE` only the specific destination table associated with that file.
    *   **Manual Run (Full Group)**: When an entire asset group is materialized manually, the `transform` asset instructs the stored procedure to `TRUNCATE` all destination tables within that group that are configured with `replace`.
    *   After truncation (if any), the procedure inserts the new data for the current `dagster_run_id` from the staging table into the destination table.
    *   **Use Case**: Ideal for full data refreshes where a destination table should be a complete snapshot of its corresponding source file. This logic prevents a run for an `append` file from accidentally clearing a `replace` table in the same group.

*   **`append`**:
    *   When the `transform` asset runs, it does **not** ask the stored procedure to truncate any tables.
    *   The procedure simply inserts the new data (for the current `dagster_run_id`) from the staging table directly into the destination table, preserving all existing data.
    *   **Use Case**: Suitable for incremental loads where each source file contains only new records to be added to the destination.
    *   **Important Note on Duplicates**: The `append` method does not automatically prevent duplicate rows in the destination table. If your source files might contain records that already exist, you should implement deduplication logic (e.g., using `MERGE` or `NOT EXISTS`) within your SQL stored procedure.
    *   **Automated Deduplication**: To simplify this, you can specify a `deduplication_key` in your `elt_pipeline_configs` table (e.g., `'ID,Date'`). If a key is provided for an `append` pipeline, the framework will automatically delete rows from the staging table that already exist in the destination table (based on matching keys) before the transformation runs. This provides a generic, code-free way to prevent duplicates.

---

### Automated `replace`-to-`append` Workflow

In many scenarios, you need to perform a full `replace` for the very first file from a source, and then `append` all subsequent files. Managing this by manually enabling and disabling pipelines is cumbersome. This framework provides a fully automated, one-time switch from `replace` to `append` mode.

*   **How it works**: You create two pipeline configurations: one for the initial `replace` and one for all subsequent `append` operations. The `replace` configuration is set to automatically deactivate itself and activate the `append` configuration after its first successful run. This is controlled by the `on_success_deactivate_self_and_activate_import` column.

#### Implementation Steps:

1.  **Create Two Configurations**: Define two rows in `elt_pipeline_configs`. They can share the same `file_pattern`.

    *   **The `replace` pipeline**: This pipeline is initially active (`is_active = 1`). Its `load_method` is `'replace'`, and you set the `on_success_deactivate_self_and_activate_import` column to the `import_name` of the `append` pipeline.
    *   **The `append` pipeline**: This pipeline is initially inactive (`is_active = 0`). Its `load_method` is `'append'`.

2.  **Reload Dagster**: After setting up the configurations, reload Dagster. Only the sensor for the active `replace` pipeline will be running.

#### Example SQL Configuration

```sql
-- 1. The 'replace' pipeline, initially active.
INSERT INTO elt_pipeline_configs (import_name, load_method, is_active, on_success_deactivate_self_and_activate_import, ...)
VALUES ('my_report_initial', 'replace', 1, 'my_report_delta', ...);

-- 2. The 'append' pipeline, initially inactive.
INSERT INTO elt_pipeline_configs (import_name, load_method, is_active, ...)
VALUES ('my_report_delta', 'append', 0, ...);
```

#### Automated Execution Flow:

1.  **Initial State**: Only the `my_report_initial` pipeline is active.
2.  **First File Drop**: You drop the first file. The sensor detects it and triggers a run for the `replace` pipeline.
3.  **Transformation and Switch**: The `transform` asset runs, truncates the destination table, and loads the data. On success, it automatically updates the database, setting `is_active=0` for `my_report_initial` and `is_active=1` for `my_report_delta`.
4.  **Next Sensor Tick**: The sensor runs again. It re-reads the active configurations from the database and now sees that only the `append` pipeline is active.
5.  **Subsequent File Drops**: You drop the next file. The sensor detects it and triggers a run for the now-active `append` pipeline. The data is correctly appended.

This provides a robust, one-time, automated switch from `replace` to `append` mode, without any manual intervention after the initial setup.

---

### Utility Asset: Automated Pipeline Setup

To dramatically accelerate the setup of new pipelines, the framework includes powerful utility assets that generate the necessary SQL scripts (DDL) and column mappings.

For each pipeline group defined in `elt_pipeline_configs`, two corresponding utility assets are created in the `_utility` asset group:
*   `[pipeline_name]_generate_setup_sql`
*   `[pipeline_name]_generate_column_mappings`

#### How It Works

When you materialize these assets, they perform a two-stage process:

1.  **DDL Generation**:
    *   The `_generate_setup_sql` asset finds a sample source file for each import in the pipeline and infers the schema (column names and data types).
    *   It generates the complete `CREATE TABLE` scripts for all staging and destination tables, plus a single, consolidated `CREATE PROCEDURE` script for the entire pipeline's transformation logic.
    *   This SQL is attached to the asset's metadata in the UI, ready for you to copy and execute. **It does not run the SQL automatically.**

2.  **Column Mapping Generation**:
    *   The `_generate_column_mappings` asset checks if the staging tables for the pipeline already exist in the database.
    *   If a table exists (meaning you have run the DDL from the previous step), it automatically generates a positional column mapping string (e.g., `'source_col_1 > table_col_1, ...'`).
    *   It then saves this mapping string directly to the `column_mapping` field in the `elt_pipeline_configs` table for the corresponding import.

#### How to Use: The Two-Run Process

Setting up a new pipeline is a simple two-run process using this single utility asset.

**Run 1: Generate the DDL**
1.  **Place Sample Files**: Ensure a source file for each import in your new pipeline exists in its `monitored_directory`.
2.  **Run the Utility Asset**: In the Dagster UI, go to the `_utility` group, find your new asset (e.g., `mynewpipeline_generate_setup_sql`), and **Materialize** it.
3.  **Copy and Execute SQL**: The run will succeed. Click the asset, find the `generated_ddl` metadata in the run history, and copy the SQL script. Execute this script in your database to create the tables and procedure.

**Run 2: Generate the Column Mapping**
1.  **Run the Mapping Asset**: In the `_utility` group, **Materialize** the corresponding column mapping asset (e.g., `mynewpipeline_generate_column_mappings`).
2.  **Done**: On this second run, the asset will see that the staging table now exists. It will automatically generate the column mapping and update the database for you.

Your pipeline is now fully configured and ready to run.

---

### Utility Asset: Automated Database Backups

To provide a safety net against accidental data loss and to maintain a version history of your critical configurations, the framework includes a utility asset that automates the backup of key database objects.

This utility, `utility_backup_database_objects`, is located in the `_utility` asset group.

**How It Works:**

When you materialize this asset, it performs the following steps:
1.  **Connects to the Database**: It uses the primary database connection.
2.  **Backs Up Table Data**: It reads all data from the following critical tables and scripts it out as a series of SQL `INSERT` statements:
    *   `elt_pipeline_configs` (Your pipeline definitions)
    *   `etl_pipeline_run_logs` (The history of all pipeline runs)
    *   `data_quality_rules` (Your data governance rules)
    *   `data_quality_run_logs` (The history of all data quality checks)
3.  **Saves to Files**: All generated SQL scripts are saved into the `backups/` directory in your project root (e.g., `backups/backup_data_elt_pipeline_configs.sql`).

These backup files can be committed to your version control system (like Git) to track changes over time or used to restore your configuration if needed.

**How to Use:**

1.  **Run the Utility Asset**:
    *   In the Dagster UI, navigate to **Assets** and select the `_utility` group.
    *   Find the asset named `utility_backup_database_objects` and click **Materialize**.
2.  **Check the `backups/` Directory**: After the run succeeds, you will find the newly created `.sql` files in the `backups/` directory.

You can run this asset manually whenever you make significant changes or add it to a schedule for regular, automated backups.

---

### Running All Utility Jobs

To streamline the setup and maintenance of your pipelines, the framework provides a single job named `utility_jobs` that materializes all assets in the `_utility` group simultaneously.

This is particularly useful when you add one or more new pipelines, as it allows you to generate DDL and column mappings for all of them in a single run.

**What It Does:**

The `utility_jobs` job will execute all assets within the `_utility` group, including:
*   `[pipeline_name]_generate_setup_sql` for all pipelines.
*   `[pipeline_name]_generate_column_mappings` for all pipelines.
*   `utility_backup_database_objects`.

**How to Use:**

1.  **Reload Dagster**: After adding or modifying pipeline configurations in the database, restart your `dagster dev` instance.
2.  **Navigate to Jobs**: In the Dagster UI, go to **Overview** -> **Jobs**.
3.  **Find and Run the Job**:
    *   Locate the job named `utility_jobs`.
    *   Click on it to open the job view.
    *   Click the **Materialize all** button in the top-right corner.

This will trigger a single run that performs all the necessary setup steps for your pipelines, making the process fast and less error-prone.

---

The core principle is to separate the "what" from the "how". The "what" (pipeline-specific details like file paths, table names) is defined in a database table, while the "how" (the execution logic for parsing, loading, and transforming) is defined in reusable Python code.

## Features

- **Metadata-Driven**: New ELT pipelines are created by simply adding a new row to a configuration table in the database. No new Dagster asset code is required for new pipelines that follow the established pattern.
- **Extensible Parsers**: A factory pattern allows for easily adding new file parsers (e.g., for Excel, JSON, Parquet) to support different source file formats.
- **Configurable Resources**: Database connections are managed through Dagster's `ConfigurableResource`, with credentials securely loaded from a `.env` file.
- **Idempotent Load**: The extract-and-load step is idempotent, truncating the staging table before each run to ensure a clean slate.
- **SQL-based Transformations**: The transformation logic is encapsulated in SQL stored procedures, allowing data engineers to work in a familiar SQL environment.
- **Automatic Asset Generation**: A factory dynamically generates a standardized set of Dagster assets for each pipeline configuration found in the database.

## Dynamic Data Governance Framework

This framework integrates a metadata-driven approach to data governance, addressing several key pillars through its architecture.

### 1. Data Quality (Strong)

The core of the governance framework is a dynamic data quality engine.

*   **Dynamic Rule Engine**: The `data_quality_rules` table allows you to define validation rules (e.g., `NOT_NULL`, `UNIQUE`, `IS_IN_SET`) as data.
*   **Automated Enforcement**: The `sp_execute_data_quality_checks` stored procedure automatically runs these rules against new data in the staging tables.
*   **Audit Trail**: The `data_quality_run_logs` table provides a complete, queryable history of all quality checks, their outcomes, and the number of failing rows.

### 2. Policy and Compliance (Good Support)

The framework provides direct mechanisms for enforcing internal data policies.

*   **Policy as Code (as Data)**: Rules in the `data_quality_rules` table represent codified data policies.
*   **Severity-Based Enforcement**: The `severity` column in a rule translates policy into action:
    *   **`FAIL`**: Enforces a strict policy by stopping the pipeline, preventing bad data from propagating.
    *   **`WARN`**: Allows for softer policies where issues are logged for review but do not halt the data flow.
*   **Compliance Auditing**: The combination of `etl_pipeline_run_logs` and `data_quality_run_logs` serves as an audit trail to prove that validation policies were executed for every run.

### 3. Data Stewardship (Good Support)

A steward can enforce that the `Position` column is never null (`FAIL`) and the `Status` is always 'A' or 'I' (`WARN`) for the `stg_ri_dbt` table with the following SQL:

```sql
-- A steward defines a critical rule to stop the pipeline on failure.
INSERT INTO data_quality_rules (rule_name, description, target_table, target_column, check_type, severity)
VALUES ('ri_dbt_position_not_null', 'Position must not be null.', 'stg_ri_dbt', 'Position', 'NOT_NULL', 'FAIL');

-- The same steward defines a warning for a less critical issue.
INSERT INTO data_quality_rules (rule_name, description, target_table, target_column, check_type, check_expression, severity) VALUES ('ri_dbt_status_in_set', 'Status must be A or I.', 'stg_ri_dbt', 'Status', 'IS_IN_SET', '''A'', ''I''', 'WARN');

---

---

## 7. Enterprise Analytics & AI Hub

This framework includes a state-of-the-art **Analytics & AI Hub** (`analytics_ui.py`) that transforms raw data into actionable intelligence. It goes beyond static reporting by integrating Machine Learning, Natural Language Processing (NLP), and prescriptive simulation directly into the data platform.

### 7.1. System Architecture & Connectivity

The analytics module operates on a **Hybrid Compute** model, leveraging the best tool for each task:

1.  **Storage Layer (SQL Server)**:
    *   Stores raw data (`stg_`, `dim_`, `fact_` tables).
    *   Stores configuration (`analytics_config`).
    *   Stores ML results (`analytics_predictions`).
    *   Performs heavy aggregations (SUM, COUNT) before data reaches Python.
2.  **Logic Layer (`core/ml_engine.py`)**:
    *   A stateless Python class containing all AI logic.
    *   **Used by Dagster**: To run batch predictions (Forecasts, Anomalies) on a schedule.
    *   **Used by Streamlit**: To run on-the-fly analysis (Clustering, NLQ) in the UI.
3.  **Presentation Layer (`analytics_ui.py`)**:
    *   A **Streamlit** web application.
    *   Connects directly to SQL Server using `SQLAlchemy`.
    *   Implements Role-Based Access Control (RBAC) using Streamlit Session State.
4.  **Orchestration Layer (`analytics.py`)**:
    *   A **Dagster Asset** (`run_predictive_analytics`) that runs periodically.
    *   Reads active configs, trains models, and saves results back to SQL.

### 7.2. Database Implementation (DDL)

To implement the analytics module, you must create the following control tables.

**1. Configuration Table (`analytics_config`)**
This table drives the Dagster automation.
```sql
CREATE TABLE analytics_config (
    id INT IDENTITY(1,1) PRIMARY KEY,
    target_table NVARCHAR(255) NOT NULL,
    date_column NVARCHAR(255) NOT NULL,
    value_column NVARCHAR(255) NOT NULL,
    model_type NVARCHAR(50) NOT NULL, -- 'anomaly_detection' or 'forecast'
    alert_webhook_url NVARCHAR(MAX) NULL,
    is_active BIT DEFAULT 1,
    created_at DATETIME DEFAULT GETUTCDATE()
);
```

**2. Predictions Table (`analytics_predictions`)**
This table stores the output of the ML models for historical tracking and visualization.
```sql
CREATE TABLE analytics_predictions (
    id INT IDENTITY(1,1) PRIMARY KEY,
    run_id NVARCHAR(255),
    target_table NVARCHAR(255),
    model_type NVARCHAR(50),
    prediction_date DATETIME,
    actual_value FLOAT NULL,
    predicted_value FLOAT NULL,
    is_anomaly BIT DEFAULT 0,
    anomaly_score FLOAT NULL,
    created_at DATETIME DEFAULT GETUTCDATE()
);
```

### 7.3. Core Components & Implementation

#### 1. 💬 Conversational Analytics (Natural Language to SQL)
*   **Implementation**: `MLEngine.generate_sql_from_question`
*   **Workflow**:
    1.  User types a question in `analytics_ui.py`.
    2.  App fetches the database schema (table/column names) using `run_query`.
    3.  App sends the schema + question to OpenAI (GPT-3.5/4).
    4.  **Security**: The returned SQL is validated against a regex whitelist (blocking `DROP`, `DELETE`) in `MLEngine.validate_sql_safety`.
    5.  The safe SQL is executed against the DB, and results are displayed.
*   **Setup**: Ensure `OPENAI_API_KEY` is set in your `.env` file.

#### 2. 🤖 Predictive Insights (Batch Processing)
*   **Implementation**: `analytics.py` (Dagster Asset)
*   **Workflow**:
    1.  The `run_predictive_analytics` asset triggers.
    2.  It queries `analytics_config` for active rules.
    3.  For each rule, it fetches the last 50,000 rows from the target table.
    4.  It calls `MLEngine.detect_anomalies` (Isolation Forest) or `MLEngine.generate_forecast` (Prophet/Holt-Winters).
    5.  Results are saved to `analytics_predictions`.
    6.  If anomalies are found and a webhook is configured, an alert is sent.

#### 3. 🛡️ Data Steward (Safe Write-Back)
*   **Implementation**: `analytics_ui.py` -> `Data Steward` Page
*   **Safety Mechanism**:
    *   Uses `st.data_editor` to allow Excel-like editing.
    *   **Critical**: Does NOT use `DELETE` + `INSERT`.
    *   **Implementation**: Uses a **SQL MERGE** strategy. It loads changes to a temp table (`#Staging_Edit`) and performs an atomic Upsert based on the Primary Key. This ensures data integrity even if the app crashes mid-operation.

#### 4. 📉 Automated Root Cause Analysis
*   **Function**: Automatically identifies the drivers behind a metric change (e.g., "Why did Sales drop yesterday?").
*   **Workflow**:
    1.  User selects a table and a metric (e.g., `TotalAmount`).
    2.  User selects a target date (e.g., the date of an anomaly).
    3.  The system compares that date to the previous period.
    4.  It scans all categorical columns to find which segments (e.g., `Region=West`, `Product=WidgetA`) contributed most to the change.

### 7.4. Developer Guide: How to Extend

#### Adding a New ML Model
1.  **Update Logic**: Add a new method to `elt_project/core/ml_engine.py` (e.g., `train_xgboost`).
2.  **Update Orchestrator**: Modify `analytics.py` to handle a new `model_type` string (e.g., `'xgboost'`) and call your new method.
3.  **Update UI**: Add `'xgboost'` to the `model_type` dropdown in `analytics_ui.py` (Configuration Manager page).

#### Running the UI Locally
The UI is a standalone Streamlit app.
```bash
# From the project root
pip install streamlit plotly openai statsmodels
streamlit run analytics_ui.py
```

---

## Security Features

To ensure the framework is robust and secure against common threats, several hardening measures have been implemented throughout the codebase.

### 1. Prevention of Arbitrary Code Execution

The ability to specify a custom parser function by name in the database (`parser_function` column) introduces a risk of arbitrary code execution. To mitigate this, the framework uses a strict whitelisting approach.

*   **Whitelisted Parsers**: In `assets/factory.py`, a set named `ALLOWED_CUSTOM_PARSERS` explicitly defines which function names from `core/custom_parsers.py` are permitted to be executed.
*   **Runtime Validation**: Before executing a custom parser, the system checks if the function name from the database configuration exists in this whitelist. If it does not, the pipeline run is halted with an error.
*   **Callable Check**: An additional check ensures that the retrieved attribute is a callable function, preventing other types of objects from being invoked.

This ensures that only pre-approved, developer-vetted custom functions can be run, preventing a user with database access from executing arbitrary code on the Dagster host.

### 2. Prevention of Path Traversal Attacks

When file paths are constructed dynamically using configuration from the database (e.g., `file_pattern`), there is a risk of a path traversal attack, where a malicious pattern like `../../../../etc/passwd` could be used to access unintended files.

*   **Path Sanitization**: The `extract_and_load_staging` asset sanitizes the `file_pattern` by using `os.path.basename()`. This strips any directory information from the pattern, ensuring that only the filename is used. The final path is then safely constructed by joining the pre-configured `monitored_directory` with the sanitized filename.

### 3. Prevention of SQL Injection

The data quality stored procedure (`sp_execute_data_quality_checks`) builds and executes SQL queries dynamically based on rules defined in the `data_quality_rules` table. To prevent SQL injection:

*   **Parameterization**: All dynamic SQL is executed using `sp_executesql`, which allows user-provided values (like `@check_expression`) to be passed as strongly-typed parameters rather than being concatenated into the query string.
*   **Object Quoting**: Table and column names, which cannot be parameterized, are safely quoted using `QUOTENAME()` to prevent them from being misinterpreted as malicious SQL commands.

### 4. Prevention of Sensitive Information Disclosure

When an error occurs, it's important to provide developers with enough information to debug without exposing potentially sensitive system details (like file paths, internal logic, or library versions) in logs that might be widely accessible.

*   **Controlled Error Logging**: In the main asset factory, the `except` block catches exceptions and writes a generic, high-level error message to the `etl_pipeline_run_logs` table in the database.
*   **Full Details in Secure Location**: The complete, detailed stack trace is still available to developers within the secure context of the Dagster UI's compute logs, but it is not persisted to the more broadly accessible SQL logging table.

4. Data Architecture (Strong) + +The entire project is built on a solid, metadata-driven data architecture that promotes good governance. + +* Standardization: The elt_pipeline_configs table enforces a standard definition for all pipelines, ensuring consistency. +* Lineage: The use of dagster_run_id provides clear data lineage, tracing every record in a staging table back to a specific pipeline run. +* Separation of Concerns: A clear separation exists between staging (raw) and destination (transformed) tables, and between Python parsing and SQL transformation logic


## Incremental Processing with `dagster_run_id`

To support incremental processing and ensure that transformation stored procedures only deal with the data from the most recent file load, a `dagster_run_id` column is automatically added to the DataFrame during the `extract_and_load_staging` step. This `run_id` is then loaded into all staging tables. The `transform` asset passes this `run_id` as a parameter to the configured SQL stored procedure, allowing the procedure to filter and process only the newly loaded data.

**Important**: All staging tables must have a `dagster_run_id NVARCHAR(255)` column defined in their schema for this feature to work correctly.

---


## Architecture

The project follows a modular structure, separating configuration, core logic, and asset definitions.

```
elt_project/
├── .env                  # Environment variables (DB credentials)
├── dagster.yaml          # Dagster instance configuration
├── pyproject.toml        # Python project dependencies
├── backups/              # Directory for database backup .sql files
├── generated_sql/        # Directory for DDL scripts generated by utility assets
│
├── elt_project/
│   ├── assets/           # Core Dagster asset definitions
│   │   ├── factory.py    # Core asset factory to generate pipelines
│   │   └── models.py     # Pydantic models for validating YAML configs
│   │   └── resources.py  # Dagster resource for SQL Server connection
│   │
│   ├── core/             # Generic, reusable business logic
│   │   ├── parsers.py    # File parser factory and implementations
│   │   └── sql_loader.py # Helper functions for loading data to SQL
│   │
│   ├── sql/
│   │   ├── 01_setup_log_table.sql
│   │   ├── 02_setup_data_governance.sql
│   │   ├── 03_manage_elt_pipeline_configs.sql
│   │   └── ...
│   │
│   └── definitions.py    # Main entry point that Dagster loads, containing asset and resource definitions
```

### Architectural Flow

1.  **Dagster Startup**: Dagster loads the code location defined in `workspace.yaml`, which points to `elt_project/definitions.py`.
2.  **Configuration Loading**: `definitions.py` executes the `load_all_assets_from_configs()` function.
3.  **Dynamic Asset Generation**:
    - This function connects to the SQL database and queries the `elt_pipeline_configs` table for active pipelines.
    - For each configuration row, it validates the data against the `PipelineConfig` Pydantic model.
    - It then calls the `create_elt_assets(config)` factory function from `assets/factory.py`.
4.  **Asset Factory**: The `create_elt_assets` function generates a pair of dependent assets for the given configuration:
    - `[pipeline_name]_extract_and_load_staging`: A Python-based asset that reads a file and loads it to a staging table.
    - `[pipeline_name]_transform`: A SQL-based asset that depends on the first.
5.  **Resource Initialization**: `definitions.py` also defines and configures the `SQLServerResource`, making it available to all assets that require it.
6.  **Sensor Generation**: If a pipeline configuration includes a `monitored_directory`, a file system sensor is dynamically generated to watch that directory for new or modified files.
6.  **Asset Execution**: When a pipeline is run in the Dagster UI:
    - The `_extract_and_load_staging` asset executes first. It uses the `ParserFactory` from `core/parsers.py` to read the source file and the `sql_loader.py` module to load the resulting DataFrame into the specified staging table.
    - Upon its successful completion, the `_transform` asset executes. It calls the configured stored procedure using `sql_loader.py` to perform the final data transformation.

---


- **SQL-based Transformations**: The transformation logic is encapsulated in SQL stored procedures, allowing data engineers to work in a familiar SQL environment.
- **Automatic Asset Generation**: A factory dynamically generates a standardized set of Dagster assets for each pipeline configuration found in the database.



## Getting Started: How to Start Everything and Ensure it Works

This guide provides a complete walkthrough from initial setup to verifying that the automated file monitoring is active and triggering pipelines correctly.

### Step 1: Prerequisites

*   **Python 3.8+**
*   A running **Microsoft SQL Server** instance.
*   **ODBC Driver for SQL Server** installed on the machine where you will run Dagster.

### Step 2: Installation

Clone the repository and install the Python dependencies in "editable" mode, which is required for Dagster to discover your code.

```bash
git clone <your-repo-url>
cd elt_project
pip install -e .
```

### Step 3: Database Setup

1.  Connect to your SQL Server instance using a tool like SSMS or Azure Data Studio.
2.  Create a new database for this project (e.g., `test`).
3.  Execute the SQL scripts located in the `elt_project/sql/` directory in the correct order to create the necessary tables and procedures:
    *   `00_setup_config_table.sql`
    *   `01_setup_tables.sql` (or similar DDL for your specific tables)
    *   `02_transformation_procs.sql` (or similar DML for your stored procedures)
    *   `03_setup_log_table.sql`

### Step 4: Environment Configuration

Create a `.env` file in the root of the project directory. This file securely stores your database credentials. Copy the template below and fill in your details.

```ini
# .env

DB_DRIVER="{ODBC Driver 17 for SQL Server}"
DB_SERVER="your_server_name_or_ip"
DB_DATABASE="test"
DB_USERNAME="your_username"
DB_PASSWORD="your_password"
DB_TRUST_SERVER_CERTIFICATE="yes"
```

### Step 5: Configure File Monitoring

To enable automatic pipeline triggers, you must tell Dagster which folders to watch.

1.  **Update the Database**: In your `elt_pipeline_configs` table, set the `monitored_directory` for the pipelines you want to automate. The path must be an **absolute path** to a folder on the machine where you will run Dagster.

    ```sql
    -- Example: Configure a pipeline to watch a specific folder.
    -- IMPORTANT: Always use forward slashes (/) for paths, even on Windows.
    UPDATE elt_pipeline_configs
    SET
        monitored_directory = 'C:/Users/YourUser/Documents/DataDrop' -- Windows example
    WHERE import_name = 'ri_dbt';
    ```

2.  **Create the Monitored Folder**: On your computer, create the folder you specified in the `monitored_directory` column (e.g., create the `DataDrop` folder inside your `Documents`). If this folder doesn't exist, the sensor will not start.

### Step 6: Run Dagster and Verify

1.  **Start Dagster**: Open a terminal in the project root and run:
    ```bash
    dagster dev
    +-------------------------------------------------+
    | Add a generic credential                        |
    +-------------------------------------------------+
    |                                                 |
    | Internet or network address:                    |
    | [ DagsterELTFramework/DB_Credentials ]          |
    |                                                 |
    | User name:                                      |
    | [ your_db_username_here ]                       |
    |                                                 |
    | Password:                                       |
    | [ ******************** ]                        |
    |                                                 |
    |                      [ OK ]   [ Cancel ]        |
    +-------------------------------------------------+
    ```

2.  **Open the UI**: Navigate to `http://localhost:3000` in your web browser.
5.  Click **OK** to save the credential.
#### After Adding Credentials
Once you have saved the credential, you can simply **re-run the `run_elt_service.bat` script**. It will now automatically and securely retrieve the credentials it needs to connect to the database.

3.  **Enable the Sensor**:
    *   Go to the **Sensors** tab in the UI.
    *   You will see a list of sensors that were dynamically generated from your database configurations (e.g., `ri_dbt_file_sensor`).
    *   **Turn the sensor on** using the toggle switch next to its name. It will now start polling the monitored directory.
### After Adding Credentials

4.  **Test the Trigger**:
    *   Find a file that matches the `file_pattern` for your configured pipeline (e.g., `RI_DBT.csv`).
    *   **Copy or move this file into the `monitored_directory`** you created in Step 5.
    *   Wait about 30 seconds (the default polling interval).
    *   Go to the **Runs** tab in the Dagster UI. You should see a new pipeline run that was automatically triggered by the sensor. This confirms that your setup is working correctly!

---

## How to Add a New Pipeline (The Easy Way)
## Developer Guide

Instead of writing SQL `INSERT` statements by hand, you can use the built-in **Configuration Generator** job to create the necessary SQL for you through a user-friendly UI.
This section is for developers who need to set up the environment manually, add new pipelines, or extend the framework's functionality.

### Step 1: Launch the Configuration Generator Job
### Initial Setup

1.  In the Dagster UI, navigate to **Overview** -> **Jobs**.
2.  Find and click on the job named `generate_pipeline_config_sql`.
3.  Click the **Launch run** button in the top-right corner. This will open the launchpad.

### Step 2: Fill Out the Configuration Form

The launchpad displays a form that mirrors the fields in the `elt_pipeline_configs` table.

1.  **Fill in the details**: Enter the `pipeline_name`, `import_name`, table names, and other metadata for your new pipeline. The form includes descriptions for each field to guide you.
2.  **Choose an Ingestion Method**: Use the `ingestion_method` selector to choose how your data is sourced (e.g., `file_system`, `sftp_download`). The form will dynamically show the required fields for your choice.
3.  **Choose a Load Method**: Select `replace` for full refreshes or `append` for incremental loads. If you choose `append`, you can optionally provide a `deduplication_key`.

 <!-- You can replace this with a real screenshot -->

### Step 3: Generate and Execute the SQL

1.  Once the form is complete, click the **Launch Run** button at the bottom of the launchpad.
2.  The job will run very quickly. Go to the **Runs** tab and click on the latest run for the `generate_pipeline_config_sql` job.
3.  Click on the `generate_config_sql_op` box in the run graph to open the details pane on the right.
4.  In the "Outputs" section, find the **generated_sql** metadata entry and click "Show Markdown".
5.  You will see the complete, formatted SQL `INSERT` statement.

     <!-- You can replace this with a real screenshot -->

6.  **Copy the SQL script** and execute it in your preferred SQL management tool (like SSMS or Azure Data Studio) to add the configuration to your database.

### Step 4: Reload Dagster

After adding the new configuration to the database, you must reload Dagster for the new assets to appear.

*   If you are running `dagster dev`, stop it (Ctrl+C) and start it again.

Your new pipeline will now be visible in the Dagster UI, ready to be used.

---

## How to Add a New Pipeline

Let's say you have a new data source, `suppliers.csv`, that you want to add as a new pipeline.

### Step 1: Add the Data File

Place your new data file, `suppliers.csv`, inside the `data/raw/` directory, or any directory you wish to monitor. The `file_pattern` in the database will specify the file name, and `monitored_directory` will specify the path.

### Step 2: Define Database Objects

You need to create the necessary database tables and the transformation logic.

1.  **Create Tables**: Add `CREATE TABLE` statements to your SQL setup scripts for both the staging table (`stg_suppliers`) and the final destination table (`dest_suppliers`). The staging table schema should match the columns in your `suppliers.csv` file.

2.  **Create Stored Procedure**: Add a `CREATE OR ALTER PROCEDURE` statement for `sp_transform_suppliers`. This procedure will contain the SQL logic to move and transform data from `stg_suppliers` to `dest_suppliers`.

3.  **Execute SQL**: Run the updated SQL scripts against your database to create the new objects.

### Step 3: Add the Pipeline Configuration to the Database

Insert a new row into the `elt_pipeline_configs` table. This tells the framework about the new pipeline. Since `suppliers.csv` is a standard CSV file, you will use the generic `csv` parser and set the `parser_function` to `NULL`.
Note: The order of columns in the `INSERT` statement must match the order in the `CREATE TABLE` definition.
> **Important**: When specifying file paths in the `monitored_directory` column, always use **forward slashes** (`/`) instead of backslashes (`\`), even on Windows.

```sql
INSERT INTO elt_pipeline_configs
    (pipeline_name, import_name, file_pattern, monitored_directory, connection_string, file_type, staging_table, destination_table, parser_function, transform_procedure, load_method, created_at, is_active, last_import_date)
VALUES
    ('Suppliers', 'suppliers_csv', 'suppliers.csv', 'C:/path/to/your/monitored/folder', NULL, 'csv', 'stg_suppliers', 'dest_suppliers', NULL, 'sp_transform_suppliers', 'replace', GETUTCDATE(), 1, NULL);
```

### Step 4: Reload Dagster

Restart the `dagster dev` process. The new "suppliers_csv" asset will appear automatically in the "Suppliers" asset group, ready to be materialized.

### Example: Adding the 'NextGen' Pipeline

This example demonstrates adding a more complex pipeline called 'NextGen', which processes data from a source file (e.g., `dynamic_sap_data.csv`) into the `dest_dynamic_sap` table.

#### Step 1: Define Database Objects

First, execute the SQL script to create the necessary staging table, destination table, and transformation stored procedure. This script defines the schema and the business logic for the transformation.

**`elt_project/sql/03_setup_NextGen_pipeline.sql`**
```sql
-- 1. Staging table for the source file
CREATE TABLE stg_dynamic_sap (
  [UniqueId] [nvarchar](255) NULL,
  [SubmitDate] [datetime] NULL,
  [CanBeCompleted] [bit] NOT NULL DEFAULT 0,
  -- ... other columns from the source file
  dagster_run_id NVARCHAR(255)
);
GO

-- 2. Destination table for the transformed data
CREATE TABLE dest_dynamic_sap (
    [SubmitDate] DATETIME,
    [CanBeCompleted] NVARCHAR(50),
    -- ... other transformed columns
    load_timestamp DATETIME DEFAULT GETUTCDATE()
);
GO

-- 3. Transformation stored procedure
CREATE OR ALTER PROCEDURE sp_transform_dynamic_sap
    @run_id NVARCHAR(255),
    @tables_to_truncate NVARCHAR(MAX) = NULL
AS
BEGIN
    -- ... (Truncation and INSERT logic as defined in the script)
END;
GO
```

#### Step 2: Add the Pipeline Configuration to the Database

Next, insert a new row into the `elt_pipeline_configs` table. This metadata entry tells the framework how to create and manage the pipeline assets.
```sql
INSERT INTO elt_pipeline_configs (
    pipeline_name, import_name,
    file_pattern, monitored_directory, file_type,
    staging_table, destination_table, parser_function, transform_procedure, load_method, is_active,
) VALUES (
    -- Core Pipeline Fields
    'NextGen',                      -- The asset group name in the Dagster UI.
    'nextgen_sap',                  -- A unique identifier for this data source.
    'dynamic_sap_data-*.csv',       -- A pattern to match source files (e.g., dynamic_sap_data-123.csv).
    'C:\\Pipelines\\NextGen',       -- The absolute path to the folder to monitor for files.
    'csv',                          -- Use the standard CSV parser.
    'stg_dynamic_sap',              -- The staging table to load raw data into.
    'dest_dynamic_sap',             -- The final destination table.
    NULL,                           -- No custom Python parser function needed.
    'sp_transform_dynamic_sap',     -- The stored procedure to run for transformation.
    'replace',                      -- Truncate the destination table before inserting new data.
    1,                              -- This pipeline is active.
);
```

#### Step 3: Reload Dagster and Verify

1.  **Create the monitored folder** (`C:\Pipelines\NextGen` in this example) on the machine running Dagster.
2.  **Restart the `dagster dev` process.**
3.  In the Dagster UI, you will now see a new asset group named "nextgen".
4.  Go to the **Sensors** tab, find the `nextgen_sap_file_sensor`, and turn it on.
5.  Drop a file named `dynamic_sap_data-001.csv` into the `C:\Pipelines\NextGen` folder. The sensor will detect it and trigger a new run, which you can monitor on the **Runs** tab.

### Handling Multiple Files Appended to a Single Staging Table

Sometimes, you have multiple source files (e.g., daily or monthly extracts) that logically belong together and should be combined into a single staging table before a unified transformation. This framework supports this by allowing multiple pipeline configurations to target the same staging table with an `append` load method.

**Example Scenario**: You receive daily sales files (`sales_2023_01_01.csv`, `sales_2023_01_02.csv`, etc.) that you want to append into a `stg_daily_sales` table, and then transform all of them into a `dest_aggregated_sales` table.

### Step 1: Add the Data Files

Place your daily sales files (e.g., `sales_2023_01_01.csv`, `sales_2023_01_02.csv`) into `data/raw/`.

### Step 2: Define Database Objects

1.  **Create Tables**: Create a single staging table (`stg_daily_sales`) and a single destination table (`dest_aggregated_sales`). The staging table schema should be able to accommodate all daily files.

2.  **Create Stored Procedure**: Create a `sp_transform_daily_sales` procedure. This procedure will read *all* data from `stg_daily_sales`, perform any necessary aggregations or cleaning, and load the result into `dest_aggregated_sales`.

3.  **Execute SQL**: Run the updated SQL scripts.

### Step 3: Add Pipeline Configurations to the Database

For each daily file, insert a separate row into `elt_pipeline_configs`. Crucially, set `load_method = 'append'` and ensure all these configurations point to the *same* `staging_table` and `transform_procedure`.

```sql
-- Configuration for sales_2023_01_01.csv (assuming it's in a monitored directory)
INSERT INTO elt_pipeline_configs
    (pipeline_name, import_name, file_pattern, monitored_directory, connection_string, file_type, staging_table, destination_table, parser_function, transform_procedure, load_method, created_at, is_active, last_import_date)
VALUES
    ('DailySales', 'sales_jan_01', 'sales_2023_01_01.csv', 'C:/data_drops/sales', NULL, 'csv', 'stg_daily_sales', 'dest_aggregated_sales', NULL, 'sp_transform_daily_sales', 'append', GETUTCDATE(), 1, NULL);

INSERT INTO elt_pipeline_configs
    (pipeline_name, import_name, file_pattern, monitored_directory, connection_string, file_type, staging_table, destination_table, parser_function, transform_procedure, load_method, created_at, is_active, last_import_date)
VALUES
    ('DailySales', 'sales_jan_02', 'sales_2023_01_02.csv', 'C:/data_drops/sales', NULL, 'csv', 'stg_daily_sales', 'dest_aggregated_sales', NULL, 'sp_transform_daily_sales', 'append', GETUTCDATE(), 1, NULL);

```

### Step 4: Reload Dagster

Restart `dagster dev`. You will see individual `_extract_and_load_staging` assets for each daily file (e.g., `sales_jan_01_extract_and_load_staging`, `sales_jan_02_extract_and_load_staging`). All these will feed into a single `sp_transform_daily_sales_transform` asset.

When a sensor triggers a run for a single file (e.g., `sales_2023_01_01.csv`):
1.  The `sales_jan_01_extract_and_load_staging` asset runs, appending its data to `stg_daily_sales`.
2.  Upon its success, the shared `dailysales_transform` asset runs. It executes the `sp_transform_daily_sales` stored procedure, passing the `dagster_run_id` associated only with the newly loaded data.

This process happens independently for each configured file. If `sales_jan_01.csv` fails to load, it will **not** prevent `sales_jan_02.csv` from being processed successfully. The stored procedure (`sp_transform_daily_sales`) will now process only the data from the current run, making it highly efficient for incremental updates.

This approach allows you to manage individual file imports while ensuring their data is consolidated for a single, unified transformation.

---

---

## How to Add a New Parser

If you need to support a new file type, like `json`:

1.  **Create a Parser Class:** Add a `JsonParser` class in `elt_project/core/parsers.py`.

    ```python
    # elt_project/core/parsers.py

    class JsonParser(Parser):
        """Parses JSON files."""
        def parse(self, file_path: str) -> pd.DataFrame:
            return pd.read_json(file_path, lines=True) # Or other options
    ```

2.  **Register the Parser:** In the same file, register your new parser with the factory.

    ```python
    # elt_project/core/parsers.py

    ...
    parser_factory.register_parser("csv", CsvParser)
    parser_factory.register_parser("psv", PsvParser)
    parser_factory.register_parser("json", JsonParser) # Add this line
    ```

You can now use `parser_type: "json"` in your YAML configuration files.

---

## Integrating Advanced Data Sources: Generic Web Scraping and API Integration

This framework includes a powerful, configuration-driven web scraper that allows you to ingest data from complex web sources without writing new Python code. By defining the scraping logic in a JSON object within your `elt_pipeline_configs` table, you can instruct the system on how to authenticate, navigate, and extract data from web pages or APIs.

This is handled by the `generic_web_scraper` custom parser function.

### How It Works

This generic parser is designed to handle both traditional web scraping (parsing HTML) and direct API integration (fetching and parsing JSON/XML). It uses standard HTTP requests and is suitable for static web pages or well-defined API endpoints.


Instead of reading a local file, the `generic_web_scraper` function interprets a JSON configuration from the `scraper_config` column in your database. This configuration tells it how to:

1.  **Authenticate**: Handle different login mechanisms, such as a form with a Time-based One-Time Password (TOTP).
2.  **Extract Data**: Fetch data from a specified URL after authentication.
3.  **Parse Data**: Convert the fetched data (e.g., from JSON or an HTML table) into a Pandas DataFrame.

### Step 1: Install Prerequisites

The generic scraper may require additional libraries depending on the extraction method.

```bash
# For all scraping
pip install requests pyotp

# For parsing HTML tables
pip install beautifulsoup4 lxml
```

### Step 2: Securely Store Credentials

Add the necessary credentials for your web source to your `.env` file. The scraper configuration will reference these environment variables by name. **Never store secrets directly in the database.**

```ini
# .env

# Example credentials for a system named 'SystemA'
SYSTEM_A_USERNAME="your_scraper_username"
SYSTEM_A_PASSWORD="your_scraper_password"
SYSTEM_A_TOTP_SECRET="YOUR_BASE32_TOTP_SECRET_KEY"
```

### Step 3: Define the Scraper Configuration JSON

This is the core of the generic approach. You will create a JSON object that describes the entire scraping process. This JSON will be stored in the `scraper_config` column of your `elt_pipeline_configs` table.

#### Example JSON for a TOTP-Authenticated Site that Returns JSON Data

```json
{
    "authentication": {
        "method": "totp_form",
        "login_url": "https://system-a.com/login",
        "payload": {
            "username_env_var": "SYSTEM_A_USERNAME",
            "password_env_var": "SYSTEM_A_PASSWORD",
            "totp_secret_env_var": "SYSTEM_A_TOTP_SECRET",
            "static_form_field": "some_static_value"
        },
        "success_condition": {
            "type": "text_in_response",
            "value": "Welcome, you are logged in"
        }
    },
    "data_extraction": {
        "url": "https://system-a.com/api/data",
        "method": "json",
        "json_path": "data.results"
    }
}
```

#### Example JSON for a Site with an HTML Table (No Authentication)

```json
{
    "authentication": {
        "method": "none"
    },
    "data_extraction": {
        "url": "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population",
        "method": "html_table",
        "table_index": 0
    }
}
```

### Step 4: Configure Your Pipeline in the Database

Insert a new row into `elt_pipeline_configs`.

1.  Set `parser_function` to `'generic_web_scraper'`.
2.  Paste your JSON configuration into the `scraper_config` column.
3.  The `file_pattern` can be a dummy value, and `monitored_directory` can be `NULL` if you trigger the asset manually.

```sql
-- Add the new column if you haven't already
ALTER TABLE elt_pipeline_configs ADD scraper_config NVARCHAR(MAX) NULL;
GO

-- Insert the pipeline configuration
INSERT INTO elt_pipeline_configs (
    pipeline_name, import_name, file_pattern, monitored_directory,
    file_type, staging_table, destination_table,
    parser_function, scraper_config, transform_procedure, load_method, is_active
) VALUES (
    'WebScraper',                   -- Asset group name
    'system_a_scraper',             -- Unique import name
    'web_scrape_trigger.txt',       -- Dummy file pattern
    NULL,                           -- No monitored directory
    'custom_scraper',               -- Descriptive file_type
    'stg_scraped_data',             -- Staging table
    'dest_scraped_data',            -- Destination table
    'generic_web_scraper',          -- The name of our generic function
    '{"authentication": {"method": "totp_form", "login_url": "...", ...}, "data_extraction": {...}}', -- PASTE YOUR JSON HERE
    'sp_transform_scraped_data',    -- Your transformation procedure
    'replace',                      -- Load method
    1                               -- Is active
);
```

### Step 5: Reload Dagster

Restart your `dagster dev` instance. The new asset will appear. When you run it, it will execute the scraping logic defined in your database configuration, providing a flexible way to add new web sources without changing any Python code.

---

## Integrating Dynamic Web Sources with Selenium (for JavaScript-Heavy Sites)

While the `requests`-based scraper is efficient for static sites and simple APIs, many modern web applications rely heavily on JavaScript to render content and handle user interactions. For these sites, a simple HTTP request is not enough. This is where **Selenium** comes in.

This framework includes a generic, configuration-driven Selenium scraper that automates a real web browser (like Chrome or Firefox) to perform complex actions, including logging in with a Time-based One-Time Password (TOTP), before extracting data.

### Why Use Selenium?

While the `generic_web_scraper` (described above) is efficient for direct API calls and static HTML parsing, the `generic_selenium_scraper` is specifically designed for scenarios where a full browser environment is required.


- **JavaScript Execution**: It can interact with elements that are loaded or enabled by JavaScript after the initial page load.
- **Complex User Actions**: It can simulate clicks, filling out forms, waiting for elements to appear, and other user behaviors.
- **Handles Complex Authentication**: It can navigate multi-step login flows that are difficult or impossible with `requests`.

### How It Works

Similar to the `requests`-based scraper, the Selenium scraper is driven by a JSON configuration stored in the `scraper_config` column of your `elt_pipeline_configs` table. You will use the `generic_selenium_scraper` custom parser function. This configuration defines a sequence of actions for the browser to perform.

### Step 1: Install Prerequisites

You'll need to install Selenium and a helper library to manage the browser drivers automatically. You will also need `pyotp` to generate TOTP codes.
The Selenium-based scraper requires a few extra libraries for browser automation (`selenium`, `webdriver-manager`), TOTP generation (`pyotp`), and HTML parsing (`beautifulsoup4`, `lxml`).

These dependencies should be added to your `pyproject.toml` file and installed by running `pip install -e .` from the project root. This ensures all required packages are installed correctly.

### Step 2: Securely Store Credentials (including TOTP Secret)

Add the necessary credentials for your web source to your `.env` file. For TOTP, you need the **Base32 secret key** that was used to set up the authenticator on your phone. You can usually get this by editing the account in your authenticator app and looking for an "export" or "show secret" option.

**Never store secrets directly in the database.**

```ini
# .env

# Example credentials for a system requiring TOTP
COMPLEX_SITE_USERNAME="your_username"
COMPLEX_SITE_PASSWORD="your_password"
COMPLEX_SITE_TOTP_SECRET="JBSWY3DPEHPK3PXP" # This is the Base32 secret key from your authenticator app
```

### Step 3: Define the Selenium Scraper Configuration JSON

This JSON object is the "script" that the generic scraper will follow. It consists of a list of actions to be performed sequentially.

#### JSON Configuration Template

Below is a comprehensive template demonstrating all available fields and action types.

```json
{
    "driver_options": {
        "headless": true
    },
    "login_url": "https://example.com/login",
    "actions": [
        {
            "type": "find_and_fill", "selector": "id", "selector_value": "username",
            "value_env_var": "EXAMPLE_USERNAME"
        },
        {
            "type": "find_and_fill_totp", "selector": "id", "selector_value": "totp_code",
            "totp_secret_env_var": "EXAMPLE_TOTP_SECRET"
        },
        { "type": "click", "selector": "css_selector", "selector_value": "button.login" },
        { "type": "wait", "duration_seconds": 5 },
        { "type": "wait_for_element", "selector": "id", "selector_value": "dashboard", "timeout": 15 },
        {
            "type": "if",
            "condition": {
                "type": "element_exists", "selector": "id", "selector_value": "popup-modal", "timeout": 3
            },
            "then": [
                { "type": "click", "selector": "xpath", "selector_value": "//button[text()='Close']" }
            ],
            "else": []
        },
        {
            "type": "while_loop",
            "condition": {
                "type": "element_exists", "selector": "id", "selector_value": "next-page-button", "timeout": 2
            },
            "max_iterations": 10,
            "loop_actions": [
                {
                    "type": "extract_and_accumulate", "target_import_name": "my_paginated_data",
                    "method": "html_table", "table_index": 0
                },
                { "type": "click", "selector": "id", "selector_value": "next-page-button" }
            ]
        }
    ],
    "data_extraction": [
        {
            "target_import_name": "my_main_data",
            "url": "https://example.com/data/main-report",
            "method": "html_table",
            "table_index": 0
        }
    ]
}
```

#### Example 1: Site with TOTP Login

This example logs into a site, waits for the dashboard to load, and then scrapes the first HTML table it finds.

```json
{
    "driver_options": {
        "headless": true
    },
    "login_url": "https://some-complex-site.com/login",
    "actions": [
        {
            "type": "find_and_fill",
            "selector": "id",
            "selector_value": "username-field",
            "value_env_var": "COMPLEX_SITE_USERNAME"
        },
        {
            "type": "find_and_fill",
            "selector": "id",
            "selector_value": "password-field",
            "value_env_var": "COMPLEX_SITE_PASSWORD"
        },
        {
            "type": "click",
            "selector": "xpath",
            "selector_value": "//button[text()='Sign In']"
        },
        {
            "type": "wait_for_element",
            "selector": "id",
            "selector_value": "totp-input-field",
            "timeout": 10
        },
        {
            "type": "find_and_fill_totp",
            "selector": "id",
            "selector_value": "totp-input-field",
            "totp_secret_env_var": "COMPLEX_SITE_TOTP_SECRET"
        },
        {
            "type": "click",
            "selector": "xpath",
            "selector_value": "//button[text()='Verify']"
        },
        {
            "type": "wait_for_element",
            "selector": "css_selector",
            "selector_value": ".dashboard-welcome-message",
            "timeout": 15
        }
    ],
    "data_extraction": [
        {
            "target_import_name": "complex_site_data",
            "method": "html_table",
            "table_index": 0
        }
    ]
}
```

### Step 4: Configure Your Pipeline in the Database

Insert a new row into `elt_pipeline_configs`.

1.  Set `parser_function` to `'generic_selenium_scraper'`.
2.  Paste your JSON configuration into the `scraper_config` column.

```sql
INSERT INTO elt_pipeline_configs (
    pipeline_name, import_name, file_pattern, monitored_directory,
    file_type, staging_table, destination_table,
    parser_function, scraper_config, transform_procedure, load_method, is_active
) VALUES (
    'SeleniumScraper',              -- Asset group name
    'complex_site_data',            -- Unique import name
    'selenium_scrape.trigger',      -- Dummy value, not used
    NULL,                           -- No monitored directory for manual runs
    'selenium_scraper',             -- Descriptive file_type
    'stg_complex_site',             -- Your staging table
    'dest_complex_site',            -- Your destination table
    'generic_selenium_scraper',     -- The name of our generic Selenium function
    '{"driver_options": {"headless": true}, "login_url": "...", ...}', -- PASTE YOUR FULL JSON HERE
    'sp_transform_complex_data',    -- Your transformation procedure
    'replace',                      -- Load method
    1                               -- Is active
);
```

### Step 5: Reload Dagster

Restart your `dagster dev` instance. The new asset will appear in the "SeleniumScraper" group. When you materialize it, it will launch a browser, execute the login and navigation steps defined in your database configuration, extract the data, and pass it down the pipeline, all without requiring new Python code for each new site.

---

## Integrating with SFTP Servers

In addition to web scraping, the framework can be extended to pull data directly from SFTP (Secure File Transfer Protocol) servers. This is achieved through a generic, configuration-driven custom parser that handles the connection, download, and parsing of remote files.

This capability is provided by the `generic_sftp_downloader` custom parser function.

### How It Works

Instead of monitoring a local directory, this function connects to an SFTP server, downloads a specified file to a temporary local directory, and then uses the framework's existing `parser_factory` to parse the downloaded file into a pandas DataFrame. The entire process is driven by a JSON configuration stored in the `scraper_config` column of your `elt_pipeline_configs` table.

### Step 1: Install Prerequisites

You will need to add a Python library for handling SFTP connections. A popular choice is `pysftp`.

```bash
pip install pysftp
```

### Step 2: Securely Store Credentials

Add the SFTP server credentials to your `.env` file. The configuration will reference these environment variables by name, ensuring no secrets are stored in the database.

```ini
# .env

# Example credentials for an SFTP server
MY_SFTP_HOST="sftp.example.com"
MY_SFTP_USER="myuser"
MY_SFTP_PASSWORD="mypassword"
# For key-based authentication, you can also specify a private key path
# MY_SFTP_PRIVATE_KEY_PATH="C:/Users/YourUser/.ssh/id_rsa"
```

### Step 3: Define the SFTP Configuration JSON

Create a JSON object that describes the SFTP connection and how the downloaded file should be parsed. This JSON will be stored in the `scraper_config` column.

```json
{
    "sftp_details": {
        "hostname_env_var": "MY_SFTP_HOST",
        "username_env_var": "MY_SFTP_USER",
        "password_env_var": "MY_SFTP_PASSWORD",
        "remote_path": "/remote/data/",
        "file_pattern": "daily_sales_*.csv"
    },
    "parse_details": {
        "file_type": "csv"
    }
}
```

### Step 4: Configure Your Pipeline in the Database

Insert a new row into `elt_pipeline_configs` to define the SFTP pipeline.

1.  Set `parser_function` to `'generic_sftp_downloader'`.
2.  Paste your JSON configuration into the `scraper_config` column.

```sql
INSERT INTO elt_pipeline_configs (
    pipeline_name, import_name, file_pattern,
    file_type, staging_table, destination_table,
    parser_function, scraper_config, transform_procedure, load_method, is_active
) VALUES (
    'SFTP_Imports',                 -- Asset group name
    'sftp_daily_sales',             -- Unique import name
    'daily_sales.csv',              -- Dummy value, as path is in config
    'custom_sftp',                  -- Descriptive file_type
    'stg_daily_sales',              -- Staging table
    'dest_daily_sales',             -- Destination table
    'generic_sftp_downloader',      -- The name of our new function
    '{"sftp_details": {"hostname_env_var": "MY_SFTP_HOST", ...}, "parse_details": {"file_type": "csv"}}', -- PASTE YOUR JSON HERE
    'sp_transform_daily_sales',     -- Your transformation procedure
    'replace',                      -- Load method
    1                               -- Is active
);
```

### Step 5: Reload Dagster

Restart your `dagster dev` instance. A new asset, `sftp_daily_sales_extract_and_load_staging`, will appear in the "SFTP_Imports" group. When you materialize this asset, it will connect to the SFTP server, download the file, parse it, and load it into the staging table, ready for transformation.

---

## Advanced Workflow: Chaining Pipelines with a Scraper

You can combine the web scraping and file monitoring features to create powerful, chained data pipelines. This is useful when a source system provides a file for download (e.g., a CSV or Excel report) via a web interface.

The workflow is as follows:

1.  **Pipeline A (Scraper)**: An asset in this pipeline uses a custom function (e.g., with Selenium) to log into a website, click a download link, and save the downloaded file to a specific directory.
2.  **Monitored Directory**: The directory where Pipeline A saves the file is the same directory that is being monitored by a sensor for **Pipeline B**.
3.  **Pipeline B (Processor)**: When the sensor detects the new file, it automatically triggers a run of Pipeline B, which then uses its standard `extract_and_load_staging` and `transform` assets to process the file.

This pattern decouples the act of *acquiring* the file from the act of *processing* it.

### Step 1: Create a Custom "File Download" Parser

First, you need a custom function that performs the download. This function won't return a DataFrame; instead, its job is to save a file to a target location. We can adapt the concept of a "parser" for this.

Add a new function to `elt_project/core/custom_parsers.py`. This example uses Selenium to log in and download a file.

```python
# elt_project/core/custom_parsers.py

def download_file_with_selenium(scraper_config_json: str) -> pd.DataFrame:
    """
    Uses Selenium to perform actions and download a file to a target directory.
    The scraper_config is extended to include a 'download_action'.
    Returns an empty DataFrame as its primary output is saving a file.
    """
    config = json.loads(scraper_config_json)
    download_path = config["download_action"]["target_directory"]

    # --- 1. Setup Selenium WebDriver with custom download path ---
    options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": download_path}
    options.add_experimental_option("prefs", prefs)
    # ... other options (headless, etc.)

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    try:
        # --- 2. Navigate and perform login actions (same as generic_selenium_scraper) ---
        driver.get(config["login_url"])
        # ... loop through config['actions'] to log in ...

        # --- 3. Perform the download click ---
        download_action = config["download_action"]
        # ... logic to find and click the download button based on 'download_action' selectors ...

    finally:
        driver.quit()

    # Return an empty DataFrame because the asset's job was to download a file, not parse it.
    return pd.DataFrame()
```

### Step 2: Configure the Two Pipelines in the Database

You will add two configurations to the `elt_pipeline_configs` table: one for the scraper (Pipeline A) and one for the file processor (Pipeline B).

```sql
-- Pipeline A: The Scraper. This pipeline downloads the file.
INSERT INTO elt_pipeline_configs (
    pipeline_name, import_name, file_pattern, monitored_directory,
    file_type, staging_table, destination_table,
    parser_function, scraper_config, transform_procedure, load_method, is_active
) VALUES (
    'Scrapers',                     -- Asset group for all scrapers.
    'download_daily_report',        -- Unique name for this download task.
    'dummy.txt',                    -- Not used, but required.
    NULL,                           -- Not triggered by a sensor.
    'custom_downloader',            -- Descriptive type.
    'stg_dummy',                    -- A dummy table, as no data is loaded.
    'dest_dummy',                   -- A dummy table.
    'download_file_with_selenium',  -- The custom function created in Step 1.
    '{"login_url": "...", "actions": [...], "download_action": {"target_directory": "C:\\\\Pipelines\\\\Incoming"}}', -- JSON config.
    'sp_dummy_transform',           -- A dummy stored procedure.
    'append',
    1
);

-- Pipeline B: The Processor. This pipeline is triggered by the downloaded file.
INSERT INTO elt_pipeline_configs (
    pipeline_name, import_name, file_pattern, monitored_directory,
    file_type, staging_table, destination_table,
    parser_function, transform_procedure, load_method, is_active
) VALUES (
    'DailyReports',                 -- Asset group for this report.
    'process_daily_report',         -- Unique name for the processing task.
    'DailyReport-*.csv',            -- The pattern of the file that gets downloaded.
    'C:\\Pipelines\\Incoming',      -- **Crucially, this is the same path as the scraper's target_directory.**
    'csv',                          -- Standard CSV parser.
    'stg_daily_report',             -- The real staging table.
    'dest_daily_report',            -- The real destination table.
    NULL,                           -- No custom parser needed here.
    'sp_transform_daily_report',    -- The real transformation logic.
    'replace',
    1
);
```

### Step 3: Reload Dagster and Run

1.  **Restart `dagster dev`**.
2.  In the UI, go to the "Scrapers" asset group and materialize the `download_daily_report` asset. This will run the Selenium script, which saves the file to `C:\Pipelines\Incoming`.
3.  The sensor for the `process_daily_report` pipeline, which is watching that folder, will detect the new file and automatically trigger a run of the "DailyReports" pipeline.

You have now successfully chained two pipelines, fully automating the process from web download to database transformation.

### Testing and Developing Scrapers Locally

Before adding a complex scraper configuration to the database, it's highly recommended to test it locally. This allows you to quickly iterate on the sequence of actions, selectors, and extraction logic. The project includes a dedicated script, `test_scraper_config.py`, for this purpose.

**How It Works:**

The `test_scraper_config.py` script runs your chosen scraper function (`generic_selenium_scraper` or `generic_web_scraper`) in isolation, using a local JSON file for configuration instead of the database. It then prints the results, including the shape and a preview of the extracted DataFrame, or a detailed error if the scrape fails.

**How to Use:**

1.  **Create a Test Configuration File**:
    *   In the root of the project, create a new file named `test_config.json`.
    *   Paste the scraper JSON configuration you are developing into this file.

2.  **Ensure Your `.env` is Ready**:
    *   The test script loads credentials (like usernames, passwords, and TOTP secrets) from the `.env` file, just like the main Dagster application. Make sure your `.env` file is present and contains the necessary variables for the site you are scraping.

3.  **Run the Test Script**:
    *   Open your terminal in the project's root directory.
    *   Execute the script, passing the path to your test configuration file. For a Selenium scraper, the command is:

1.  **Prerequisites**: Ensure you have Python 3.8+, a running Microsoft SQL Server instance, and the appropriate ODBC Driver installed.
2.  **Installation**: Clone the repository and install dependencies in editable mode:
    ```bash
    python test_scraper_config.py test_config.json
    git clone <your-repo-url>
    cd data_pipeline_automation
    pip install -e .
    ```
    *(Note: `--scraper_type selenium` is the default, so it is optional)*
3.  **Database Setup**: Connect to your SQL Server instance, create a database (e.g., `dagster_elt_framework`), and execute the scripts in the `elt_project/sql/` directory in numerical order.
4.  **Environment Configuration**: Create a `.env` file in the project root and fill in your database connection details.

4.  **Review the Output and Iterate**:
    *   **On Success**: The script will print a preview of the scraped data. Check that the columns and data are what you expect.
    *   **On Failure**: The script will print a detailed error message and a full stack trace. Use this information to debug your JSON configuration (e.g., fix a selector, adjust a wait time).
    *   Continue to modify your `test_config.json` file and re-run the script until the scraper works correctly.
### Adding a New Pipeline (The Easy Way)

5.  **Update the Database**:
    *   Once you are satisfied with the scraper's performance, copy the final, working JSON from `test_config.json` and paste it into the `scraper_config` column of the appropriate row in your `elt_pipeline_configs` table.
This framework uses utility assets to automate the most tedious parts of creating a new pipeline: writing DDL and configuring column mappings.

This local testing workflow is significantly faster than running a full Dagster materialization for each change and is the recommended way to develop new scraper configurations.
1.  **Configure in Database**: Add a new row to your `elt_pipeline_configs` table. At a minimum, specify names, file patterns, and table names.
2.  **Generate DDL**: Place a sample source file in the configured `monitored_directory`. Restart `dagster dev`, go to the `_utility` asset group, and materialize the `[pipeline_name]_generate_setup_sql` asset. Copy the generated SQL from the asset's metadata and execute it in your database.
3.  **Generate Column Mappings**: Go back to the `_utility` group and materialize the `[pipeline_name]_generate_column_mappings` asset. It will automatically generate and save the column mapping to the database.

## How to Use Custom Parsers
Your new pipeline is now fully configured and ready to process files.

Beyond adding new generic parser types (like `csv`, `json`, `psv`), you might encounter specific data sources that require unique, one-off parsing logic. This could be due to:

Good reasons to use a custom parser include:
- The file has a dynamic number of header or footer rows that must be programmatically removed.
- The file is not a standard delimited format and requires complex logic (e.g., using regular expressions) to extract fields from each line.
- The file requires a specific, non-standard character encoding that needs to be handled carefully.

### 1. Define Your Custom Parser Function

Create or modify the file `elt_project/elt_project/core/custom_parsers.py`. This file is specifically designed to house these bespoke parsing functions.

Each custom parser function must:
- Accept a single argument: `file_path` (a string representing the path to the source file).
- Return a `pandas.DataFrame`.

**Example `elt_project/elt_project/core/custom_parsers.py`:**

This example handles a CSV file that has 2 header rows to skip and a variable-length footer that starts with the text "END OF REPORT".

```python
import pandas as pd

def parse_report_with_footer(file_path: str) -> pd.DataFrame:
    """
    Parses a CSV that has a 2-line header and a dynamic footer.
    ```
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
```

### 2. Configure Your Pipeline in the Database

Instead of relying on the `file_type` column and the `parser_factory`, you can instruct a specific pipeline to use your custom function. This is done by setting the `parser_function` column in the `elt_pipeline_configs` table to the exact name of your custom function.

If the `parser_function` column is `NULL` for a given pipeline, the system will fall back to using the `file_type` and the `parser_factory` as usual.

**Example SQL `INSERT` or `UPDATE` statement:**

```sql
-- To use 'parse_report_with_footer' for a specific pipeline:
UPDATE elt_pipeline_configs
SET parser_function = 'parse_report_with_footer'
WHERE import_name = 'my_special_report';

-- When inserting a new pipeline that requires a custom parser:
INSERT INTO elt_pipeline_configs
    (pipeline_group, import_name, source_file_path, file_type, staging_table, destination_table, transform_procedure, load_method, parser_function)
VALUES
    ('MyGroup', 'my_special_report', 'data/raw/special_report.csv', 'csv', 'stg_special_report', 'dest_special_report', 'sp_transform_special_report', 'replace', 'parse_report_with_footer');

-- For a standard pipeline without a custom parser, ensure the column is NULL:
INSERT INTO elt_pipeline_configs (..., parser_function) VALUES (..., NULL);
```

### 3. Reload Dagster

After defining your custom parser function and updating the database configuration, restart your Dagster instance (`dagster dev`) for the changes to be picked up. When the asset for that specific pipeline runs, it will now invoke your custom parsing logic.
You can now use `parser_type: "json"` in your YAML configuration files.

---

## Purpose of the `id` Field in `elt_pipeline_configs`

The `id` field in the `elt_pipeline_configs` table serves as the **primary key** for the table, providing several key functions:

1.  **Unique Identification**: As a `PRIMARY KEY`, it guarantees that each row (i.e., each pipeline configuration) has a unique identifier.
2.  **Auto-Incrementing**: The `IDENTITY(1,1)` property ensures that the database automatically assigns a unique integer value to `id` for each new row inserted, starting from 1 and incrementing by 1. This removes the need for manual ID management.
3.  **Data Integrity**: It enforces entity integrity, a fundamental principle of relational databases, ensuring that every record is uniquely identifiable.
4.  **Referential Integrity (Potential)**: While not explicitly used as a foreign key in other tables within this project's current scope, a primary key like `id` is crucial for establishing relationships. For instance, a future `pipeline_run_logs` table could use `config_id` as a foreign key referencing `elt_pipeline_configs.id`.
5.  **Efficient Data Retrieval**: Database systems leverage primary keys to create indexes, which significantly accelerate data retrieval operations when querying for specific configurations.

In essence, `id` is a standard, best-practice database column that provides a stable, unique, and efficient way to reference and manage individual pipeline configurations within your metadata store.


# Dynamic, Metadata-Driven ELT Pipelines

This project provides a one-click runner (`run_elt_service.bat`) to automate the setup and execution of a Dagster-based ELT framework. It handles environment checks, code updates, dependency installation, and service startup, making it easy for end-users to run the data pipelines.

## Prerequisites

Before running the service, please ensure you have the following software installed on your system:

1.  **Python**: Required to run the core application and its dependencies. You can download it from python.org.
    - **Important**: During installation, make sure to check the box that says **"Add Python to PATH"**.
2.  **Git**: Required for automatically updating the project code. You can download it from git-scm.com.

The runner script will verify that both are installed correctly.

## How to Run

1.  Navigate to the project folder in File Explorer.
2.  Double-click the `run_elt_service.bat` file.
### 1. Start the System
Run the launcher script (e.g., `Launch Data Importer.bat`). This starts:
*   The Dagster Daemon (for sensors).
*   The Dagster Web Server (UI).
*   The Simple UI (User-facing status page).

The script will handle the rest. A command prompt window will open and guide you through the setup process. The first time you run it, it may take a few minutes to download and install the necessary packages.
### 2. Triggering Pipelines
*   **Automatic**: Simply drop a file matching the `file_pattern` into the `monitored_directory`. The sensor will pick it up within 30 seconds.
*   **Manual**: Use the Simple UI or Dagster UI to manually trigger a run for existing files.

Once started, the script will open the Dagster UI in your web browser at `http://localhost:3000`. To stop the services, simply close the command prompt window.

---

## Credential Management: Using Windows Credential Manager
## Troubleshooting

To connect to the database securely without asking for your password every time, the service uses the built-in Windows Credential Manager. This is a secure, one-time setup.
### Common Issues

If the runner script cannot find the credentials it needs, it will display a warning and instructions. Follow the steps below to add them.
**1. "File not processing"**
*   Check `Launch Data Importer.log`.
*   Ensure the file matches the `file_pattern` exactly (glob patterns are case-sensitive on some systems).
*   Ensure the sensor is running in the Dagster UI.

### Step-by-Step Guide to Add Credentials
**2. "Data replaced instead of appended"**
*   Check the `load_method`.
*   If it's `REPLACE`, ensure the files were dropped within the 2-minute batch window if you intended them to be a batch. If they are dropped 5 minutes apart, the system treats them as separate replacement events.

1.  Open the **Start Menu** and type "**Credential Manager**", then open it.
**3. "Lock timeout"**
*   If a pipeline is stuck waiting for a lock, check if another long-running pipeline is writing to the same destination table.

    
### Debugging Tools
*   **Simple UI**: Go to `http://localhost:3000` (or your configured port) to see the status of the system and trigger imports.
*   **Dagster UI**: Go to `http://localhost:3000/dagster` (default) for detailed execution graphs and logs.

2.  Select **Windows Credentials**.

    

3.  Click on **Add a generic credential**.

    

4.  Fill in the three fields exactly as described below:

    -   **Internet or network address:**
        ```
        DagsterELTFramework/DB_Credentials
        ```
        *(This must match exactly what the script is looking for.)*

    -   **User name:**
        Your database username (e.g., `sql_user_name`).

    -   **Password:**
        Your database password.

    Your completed form should look like this:

    ```
    +-------------------------------------------------+
    | Add a generic credential                        |
    +-------------------------------------------------+
    |                                                 |
    | Internet or network address:                    |
    | [ DagsterELTFramework/DB_Credentials ]          |
    |                                                 |
    | User name:                                      |
    | [ your_db_username_here ]                       |
    |                                                 |
    | Password:                                       |
    | [ ******************** ]                        |
    |                                                 |
    |                      [ OK ]   [ Cancel ]        |
    +-------------------------------------------------+
    ```

5.  Click **OK** to save the credential.

### After Adding Credentials

Once you have saved the credential, you can simply **re-run the `run_elt_service.bat` script**. It will now automatically and securely retrieve the credentials it needs to connect to the database.

### GitHub Authentication for Automatic Updates

The runner script automatically attempts to download the latest version of the application using `git pull`. For this to work, you need to authenticate with GitHub. GitHub no longer supports password authentication for Git operations; you must use a **Personal Access Token (PAT)**.

The recommended way to set this up is to store your PAT in the Windows Credential Manager.

#### Step 1: Generate a Personal Access Token (PAT)

1.  Go to the Personal access tokens page on GitHub.
2.  Click **"Generate new token"** and select **"Generate new token (classic)"**.
3.  Give the token a **Note** (e.g., "Data Pipeline Automation").
4.  Select the **`repo`** scope, which is required for pulling code.
5.  Click **"Generate token"** and **copy the token immediately**. You will not be able to see it again.

#### Step 2: Add the GitHub Credential

1.  Open **Credential Manager** and select **Windows Credentials**.
2.  Click **"Add a generic credential"**.
3.  Fill in the fields as follows:
    -   **Internet or network address:**
        ```
        git:https://github.com
        ```
    -   **User name:**
        Your GitHub username (e.g., `Data-Vlad`).
    -   **Password:**
        **Paste the Personal Access Token** you just copied.

    Your completed form should look like this:
    ```
    +-------------------------------------------------+
    | Add a generic credential                        |
    +-------------------------------------------------+
    | Internet or network address: [ git:https://github.com ] |
    | User name:                   [ YourGitHubUsername ]    |
    | Password:                    [ ******************** ]    |
    +-------------------------------------------------+
    ```
4.  Click **OK**.

---

## Migrating to a Different Orchestrator (e.g., Airflow, Prefect, Luigi)
## Developer Notes

This project is built with Dagster, leveraging its asset-centric view, dynamic asset generation, and sensor capabilities. If you decide to switch to a different orchestrator, the core business logic for data processing (parsing, SQL loading, SQL transformations) is largely decoupled and reusable. However, the orchestration layer itself would require significant changes.
### Adding Custom Parsers
1.  Add your python function to `elt_project/assets/custom_parsers.py`.
2.  Register the function name in the `ALLOWED_CUSTOM_PARSERS` set in `elt_project/assets/factory.py`.
3.  Update the `parser_function` column in the database for your import.

Here's a breakdown of what would need to be changed:

1.  **Asset Definitions (`elt_project/assets/factory.py`)**:
    *   **Dagster Decorators**: The `@asset`, `@multi_asset` decorators and `AssetExecutionContext` context object are Dagster-specific. These would need to be replaced with the equivalent task/operator definitions of the new orchestrator (e.g., Airflow's `PythonOperator`, `BashOperator`, Prefect's `@task` decorator).
    *   **Dagster Decorators**: The `@asset` and `AssetExecutionContext` context object are Dagster-specific. These would need to be replaced with the equivalent task/operator definitions of the new orchestrator (e.g., Airflow's `PythonOperator`, Prefect's `@task` decorator).
    *   **Asset Factory Logic**: The `create_extract_and_load_asset` and `create_transform_asset` functions dynamically generate Dagster assets. This factory logic would need to be re-implemented to generate the new orchestrator's task/job definitions.
    *   **Dependencies**: The way dependencies are expressed (`deps` parameter in Dagster's `@asset`) would need to be translated to the new orchestrator's dependency management system (e.g., Airflow's `>>` or `set_upstream`/`set_downstream` operators, Prefect's task flow dependencies).

2.  **Resource Management (`elt_project/assets/resources.py`)**:
    *   **Dagster Resources**: The `SQLServerResource` is a `ConfigurableResource` in Dagster. This concept would need to be replaced with the new orchestrator's way of managing external connections or configurations (e.g., Airflow Connections, Prefect Blocks). The underlying `sqlalchemy.create_engine` logic would remain, but its instantiation and passing to tasks would change.

3.  **Definitions and Job Generation (`elt_project/definitions.py`)**:
    *   **Dagster `Definitions` Object**: The `Definitions` object is the central entry point for Dagster. This would be replaced by the new orchestrator's main definition file (e.g., an Airflow DAG file, a Prefect flow script).
    *   **Job Creation**: The `define_asset_job` and `AssetSelection` constructs are Dagster-specific. The logic to group tasks into executable jobs/DAGs would need to be rewritten using the new orchestrator's APIs.
    *   **Job Creation**: The logic to group tasks into executable jobs/DAGs would need to be rewritten using the new orchestrator's APIs.
    *   **Configuration Loading**: The `load_all_definitions_from_db` function would still query the `elt_pipeline_configs` table, but instead of generating Dagster assets, it would generate the new orchestrator's task/job objects.

4.  **Sensors (`elt_project/sensors.py`)**:
    *   **Dagster Sensors API**: The `@sensor` decorator, `SensorEvaluationContext`, `RunRequest`, and `SkipReason` are all part of Dagster's sensor API. This entire file would need to be rewritten to use the new orchestrator's scheduling and triggering mechanisms (e.g., Airflow Sensors, Prefect deployments with schedules/triggers). The file system monitoring logic (`os.listdir`, `fnmatch`, `os.path.getmtime`) could be reused, but how it initiates a run would change.

5.  **Deployment and Configuration Files**:
    *   **`workspace.yaml`**: This file is specific to Dagster's code location loading and would be removed.
    *   **`dagster.yaml`**: This file configures the Dagster instance and would be replaced by the new orchestrator's configuration files.
    *   **CLI Commands**: Commands like `dagster dev` would be replaced by the new orchestrator's CLI commands (e.g., `airflow dags unpause`, `prefect deploy`).

6.  **Core Business Logic (Largely Reusable)**:
    *   **`elt_project/core/parsers.py`**: The `Parser` classes and `ParserFactory` are generic Python code and would likely remain unchanged.
    *   **`elt_project/core/custom_parsers.py`**: Custom parsing functions are pure Python and would remain unchanged.
    *   **`elt_project/core/sql_loader.py`**: Functions for loading DataFrames to SQL and executing stored procedures are generic Python/SQLAlchemy and would remain unchanged.
    *   **`elt_project/assets/models.py`**: Pydantic models for `PipelineConfig` are generic Python and would remain unchanged.
    *   **`elt_project/sql/`**: All SQL DDL and stored procedures are database-specific and would remain unchanged.

In summary, while the data processing "engines" (`core` and `sql` directories) are largely portable, the "control panel" (Dagster-specific asset definitions, resources, sensors, and deployment configurations) would need a complete overhaul to integrate with a different orchestration system.

## ETL Pipeline Logging: Using Both Dagster and SQL Server

For comprehensive and robust logging of your ELT pipelines, it is highly recommended to leverage both Dagster's built-in logging mechanisms and a dedicated SQL Server table. This hybrid approach allows you to capitalize on the strengths of each system.

### 1. Dagster's Built-in Logging (Operational Visibility & Debugging)

Dagster provides powerful logging capabilities that are deeply integrated with its UI and metadata system.

*   **Immediate UI Integration**: Logs are directly viewable within the Dagster UI alongside the asset runs, making debugging and monitoring very convenient.
*   **Structured Events**: Dagster's event log is highly structured, automatically capturing operational events like asset materializations, run start/end, step success/failure, and messages from `context.log.info()`. This allows for powerful filtering, searching, and analysis within the UI.
*   **Developer-Friendly**: Using `context.log.info()`, `context.log.error()`, etc., directly within your asset code integrates seamlessly with Dagster's logging infrastructure.

### 2. Dedicated SQL Server Table (Business Metrics, Auditing & Analytics)

For logs that require more complex querying, integration with other business intelligence tools, or serve as an audit trail for data quality and processing, a dedicated SQL Server table is an excellent complement.

*   **Customizable Schema**: You have full control over the log table's schema, allowing you to capture specific metrics and details (e.g., row counts, data quality scores, specific error codes, processing times) that are critical for business reporting or auditing.
*   **Powerful SQL Querying**: Leverage the full power of SQL for complex analytics, aggregations, and custom reports on pipeline performance, data quality, and processing metrics.
*   **BI Tool Integration**: Easily connect BI tools (e.g., Power BI, Tableau) directly to your log tables for dashboards and alerts, providing business users with insights into data pipeline health and data quality.
*   **Detailed Error Tracking**: The log table includes `error_details` (for full stack traces or detailed error messages) and `resolution_steps` (for actionable advice), significantly aiding in debugging and incident response.
*   **Long-term Archival**: SQL Server is designed for robust data storage and retrieval, suitable for long-term log retention and historical analysis.

### Why Use Both?

*   **Dagster logs** provide operational visibility, telling you *how* the pipeline ran from a technical perspective (success/failure, step execution, code-level messages).
*   **SQL Server logs** provide business-centric insights, telling you *what* happened to the data (row counts, data quality, specific business-related outcomes) in a format easily consumable by other systems and users.

This combined approach ensures you have both the immediate operational detail needed for pipeline management and the rich, queryable data needed for business intelligence and auditing.

---

## Offline and Air-Gapped Operation

The core components of this Dagster project are designed to run entirely offline or in an air-gapped environment, but the functionality of specific pipelines depends on their external dependencies.

### What Works Offline

1.  **Dagster's Core Services**: The `dagster dev` command, which starts the web UI (Dagit) and the Dagster daemon (for scheduling), runs on your local machine. You can access the UI at `http://localhost:3000`, define assets, trigger runs, and view logs without any internet connection.
2.  **Local Database Connection**: If your SQL Server database (configured in `.env`) is running on `localhost` or on your local area network (LAN), Dagster will be able to connect to it, read pipeline configurations, and write logs without issue.
3.  **Local File Monitoring**: The file sensors monitor local file system directories. This functionality is entirely local and does not require an internet connection. A sensor will correctly detect a new file and trigger a pipeline run.
4.  **Local File Processing**: Pipelines that read from and process files already on your local machine (e.g., CSV, PSV, Excel files) will work perfectly.

### What Will NOT Work Offline

1.  **Cloud-Based Database**: If your `DB_SERVER` in the `.env` file points to a cloud-hosted database (like Azure SQL or AWS RDS), Dagster will be unable to connect without an internet connection.
2.  **Web Scraping Assets**: The `generic_web_scraper` and `generic_selenium_scraper` assets are designed to connect to external websites and APIs. Any asset that uses these functions will fail because it cannot reach the internet.
3.  **Initial Dependency Installation**: Running `pip install -e .` requires an internet connection to download packages (like `dagster`, `pandas`, `sqlalchemy`, etc.) from PyPI. Once the virtual environment is set up, it can be used offline.
4.  **Selenium WebDriver Downloads**: The `generic_selenium_scraper` uses `webdriver-manager` to automatically download the correct browser driver (e.g., `chromedriver`). The first time you run a Selenium asset, it will try to download this driver. If you are offline and the driver is not already cached, the run will fail.

### Summary

Your Dagster application is a hybrid. The orchestration and local file processing parts are perfectly capable of running offline. However, any parts of your system designed to interact with network resources—be it a cloud database or a public website—will naturally fail without a network connection.

**In your specific project, you can run all your local file-based ELT pipelines offline, as long as your SQL Server instance is also running locally.** Any pipeline configured to use a web scraper will fail.
Once you have saved the credential, you can simply **re-run the `run_elt_service.bat` script**. It will now automatically and securely retrieve the credentials it needs to connect to the database.

# Data Import Service

This project provides a user-friendly web interface for triggering and monitoring Dagster data import pipelines.

## How It Works

The system is launched via the `Launch Data Importer.bat` script, which performs the following steps:
1.  Checks for prerequisites (Python, Git).
2.  Pulls the latest application code from the Git repository.
3.  Installs or updates required Python packages.
4.  Retrieves database credentials securely from the Windows Credential Manager.
5.  Configures the environment (`.env` file).
6.  Starts the simple web UI for users to interact with.

## Restricting User Access to Pipelines

By default, the user interface displays all active pipelines defined in the `elt_pipeline_configs` table. You can restrict which pipelines a specific user can see by creating a permissions table in the database and granting access explicitly.

This is an optional feature. If the permissions table does not exist, all users will continue to see all active pipelines.

### Step 1: Create the Permissions Table

Connect to your SQL database and run the following command to create the `elt_user_permissions` table. This table will store which users have access to which pipelines.

```sql
CREATE TABLE elt_user_permissions (
    id INT IDENTITY(1,1) PRIMARY KEY,
    username NVARCHAR(255) NOT NULL,
    pipeline_name NVARCHAR(255) NOT NULL,
    CONSTRAINT UQ_user_pipeline UNIQUE (username, pipeline_name)
);
```

### Step 2: Grant Access to a User

To grant a user access to one or more pipelines, insert rows into the new table. The `username` must match the database username stored in the Windows Credential Manager for that user.

For example, to grant a user named `data_entry_clerk` access to the `sap_customer_imports` and `inventory_scrapes` pipelines, you would run:

```sql
INSERT INTO elt_user_permissions (username, pipeline_name) VALUES ('data_entry_clerk', 'sap_customer_imports');
INSERT INTO elt_user_permissions (username, pipeline_name) VALUES ('data_entry_clerk', 'inventory_scrapes');
```

Now, when the user `data_entry_clerk` launches the Data Import Service, they will only see the "sap_customer_imports" and "inventory_scrapes" pipeline groups in the UI. Other users will not be affected unless they also have entries in this table.


# Data Pipeline Automation Tool

## 1. Overview

This project is a comprehensive data pipeline automation tool designed for ease of use and maintenance. It provides a simple web-based user interface for non-technical users to trigger complex data import jobs.

The entire system is packaged into a single, self-updating executable installer for effortless distribution and setup.

---

## 2. Features

*   **One-Click Installer**: Distributed as a single `DataImporter-Setup.exe` file.
*   **Automatic Prerequisite Checks**: The launcher verifies that Python and Git are installed.
*   **Self-Updating Application**: On every launch, the application automatically pulls the latest code changes using Git, ensuring users always have the most recent version.
*   **Self-Updating Launcher**: The launcher script itself can be updated automatically.
*   **Secure Credential Management**: Database credentials are not stored in code; they are securely managed using the built-in Windows Credential Manager.
*   **Simple Web UI**: An intuitive web interface allows users to select and run specific data import jobs without needing any technical knowledge.
*   **Dynamic Configuration**: The UI dynamically populates the available import jobs by reading a configuration table from the database.
*   **Developer Mode**: A dedicated mode for developers to easily test and debug the underlying Dagster data pipeline.

---

## 3. Technology Stack

*   **Data Pipeline**: Dagster
*   **Web Interface**: Flask & Waitress
*   **Database Interaction**: SQLAlchemy
*   **Core Language**: Python
*   **Launcher & Automation**: Windows Batch Script
*   **Version Control & Updates**: Git
*   **Installer Packaging**: Windows IExpress

---

## 4. For the End-User

### First-Time Installation

You will receive a single file named `DataImporter-Setup.exe`.

1.  **Run the Installer**: Double-click `DataImporter-Setup.exe`. A console window will appear.
2.  **Initial Setup**: The installer will automatically:
    *   Extract all necessary application files to a folder on your computer.
    *   Check if Python and Git are installed.
    *   Pull the latest version of the application from the central code repository.
    *   Install all required Python packages.
3.  **Database Configuration (First Run Only)**:
    *   The script will ask you to enter the **Database Server** and **Database Name**. You only need to do this once.
    *   Example:
        ```
        First-time setup: Please provide your database connection details.
          Enter Database Server (e.g., yourserver.database.windows.net): my-db-server.database.windows.net
          Enter Database Name (e.g., YourDatabaseName): ProductionDB
        ```
4.  **Credential Setup (First Run Only)**:
    *   The script will then check for database credentials. If they are not found, it will display a `WARNING` message with instructions.
    *   Follow the on-screen instructions to add your database username and password to the **Windows Credential Manager**. This is a secure, one-time setup.
        *   Open the Start Menu and search for "Credential Manager".
        *   Select "Windows Credentials".
        *   Click "Add a generic credential".
        *   The "Internet or network address" must exactly match the `CREDENTIAL_TARGET` provided in the warning message.
        *   Enter your database username and password and click OK.
    *   After adding the credential, re-run the installer.
5.  **Shortcut Creation**: Once setup is complete, the script will create a **"Launch Data Importer"** shortcut on your Desktop.
6.  **Application Start**: The web interface will automatically open in your default web browser.

### Daily Use

1.  Double-click the **"Launch Data Importer"** shortcut on your Desktop.
2.  A console window will briefly appear, showing that the application is checking for updates and starting.
3.  The web UI will open in your browser. You can now select and run your desired data imports.

**You never need to run the `DataImporter-Setup.exe` file again.** The application will update itself automatically every time you use the desktop shortcut.

---

## 5. For the Developer

### Prerequisites

*   Python 3.8+
*   Git
*   An IDE (e.g., Visual Studio Code)

### File Structure

*   `Launch Data Importer.bat`: The main launcher script for users and developers.
*   `build_installer.bat`: **Developer-only** script to create the distributable `.exe` installer.
*   `installer_config.sed`: Configuration file for `IExpress`, generated once.
*   `simple_ui.py`: The Flask application for the web interface.
*   `get_credentials.py`: Helper script to fetch credentials from Windows Credential Manager.
*   `create_dirs.py`: Helper script to create monitored directories based on database configuration.
*   `elt_project/`: The core Dagster project containing all pipeline definitions and assets.
*   `.env`: Configuration file for database connection details. **(Do not commit secrets here)**.
*   `.gitignore`: Specifies files and folders to be ignored by Git.

### Local Development Setup

1.  **Clone the Repository**:
    ```bash
    git clone <your-repository-url>
    cd data_pipeline_automation
    ```
2.  **Configure Environment**:
    *   The `Launch Data Importer.bat` script will help you create the `.env` file on the first run. This file stores non-secret configuration like the server and database name.
3.  **Set Up Credentials**:
    *   Add your development database credentials to the Windows Credential Manager as described in the user section. Ensure the `CREDENTIAL_TARGET` in the `.env` file matches the address you use in Credential Manager.
# Data Importer UI

The Data Importer is a simple, user-friendly web application designed to allow non-technical users to trigger and monitor data import jobs. It provides a clean interface that abstracts away the complexity of the underlying Dagster data pipelines.

## Prerequisites

Before running the application, ensure the following are installed on your Windows machine and accessible from the command line:

- **Python**: [Download Python](https://www.python.org/downloads/)
- **Git**: [Download Git](https://git-scm.com/downloads/)

The launcher script will handle the rest of the setup, including creating a virtual environment and installing required Python packages.

---

## Configuration

The application requires two configuration steps to connect to the database and run properly.

### 1. The `.env` File

This file tells the application which database server and database to connect to. It must be located in the same directory as the `Launch Data Importer.bat` script and should look like this:

```
DB_SERVER=YOUR_DATABASE_SERVER_NAME
DB_DATABASE=YOUR_DATABASE_NAME
```

Replace `YOUR_DATABASE_SERVER_NAME` with the actual name or IP address of your SQL Server instance. The value you set for `DB_SERVER` is **critical** for the next step.

### 2. Windows Credential Manager Setup

To avoid storing passwords in plain text, the application securely retrieves the database username and password from the Windows Credential Manager. You must add a "Generic Credential" that matches the `DB_SERVER` name from your `.env` file.

**Follow these steps very carefully:**

1.  **Open Credential Manager:**
    - Press the **Windows Key**, type `Credential Manager`, and press Enter.

2.  **Select Windows Credentials:**
    - In the Credential Manager window, click on **"Windows Credentials"**.

    

3.  **Add a Generic Credential:**
    - Click on the link that says **"Add a generic credential"**.

    

4.  **Enter the Credential Details:**
    This is the most important step. The details must be entered exactly as described.

    -   **Internet or network address:**
        -   Enter the **exact same server name** that you put for `DB_SERVER` in your `.env` file. For example, if your `.env` file has `DB_SERVER=SQLPROD01`, you must enter `SQLPROD01` here.

    -   **User name:**
        -   Enter the database username you use to log in (e.g., `your_sql_username`).

    -   **Password:**
        -   Enter the password for that database user.

    

5.  **Save the Credential:**
    - Click **OK**. The credential is now securely stored.

The application will now be able to find and use these credentials to connect to the database.

---

## Running the Application

To start the application, simply double-click the **`Launch Data Importer.bat`** file.

A command window will appear, showing the progress of the startup sequence. Once it's finished, it will automatically open the Data Importer UI in your default web browser at `http://localhost:3000`.

A shortcut to this batch file will also be created on your Desktop for easy access.
### Project Structure
*   `elt_project/assets/factory.py`: Core logic for generating assets.
*   `elt_project/sensors.py`: File sensor logic.
*   `elt_project/definitions.py`: Main entry point for Dagster.
*   `simple_ui.py`: The Flask-based user interface.
