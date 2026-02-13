import os
import sys
import json
import argparse
from dotenv import load_dotenv
import pandas as pd

# Add the project root to the Python path to allow imports from elt_project.
# This allows the script to be run from the project root directory, e.g., `python utils/test_scraper_config.py ...`
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    # It's good practice to keep potentially heavy imports inside the try block
    # if they are only needed for the script's core functionality.
    from elt_project.assets.custom_parsers import generic_selenium_scraper
    # from elt_project.core.custom_parsers import generic_web_scraper # Uncomment if you have a requests-based scraper
except ImportError as e:
    # Do not use an f-string to print the error 'e' directly, as the error message
    # from Dagster may contain curly braces that break f-string formatting.
    print("Error: Could not import scraper functions. Make sure you are running this script from the project root directory.")
    print("Details:", e)
    sys.exit(1)

def main():
    """
    Main function to execute the scraper test.
    """
    # --- Setup Argument Parser ---
    parser = argparse.ArgumentParser(
        description="Test a scraper's JSON configuration outside of a full Dagster run."
    )
    parser.add_argument(
        "config_file",
        type=str,
        help="Path to the JSON file containing the scraper configuration."
    )
    parser.add_argument(
        "--scraper_type",
        type=str,
        choices=["selenium"], # Add "requests" here if you implement generic_web_scraper
        default="selenium",
        help="The type of scraper function to test ('selenium' or 'requests')."
    )
    args = parser.parse_args()

    # --- Load Environment and Config ---
    print("Loading environment variables from .env file...")
    load_dotenv()

    print(f"Reading scraper configuration from: {args.config_file}")
    try:
        with open(args.config_file, 'r') as f:
            scraper_config_json = f.read()
            # Validate that it's valid JSON
            json.loads(scraper_config_json)
    except FileNotFoundError:
        print(f"Error: Configuration file not found at '{args.config_file}'")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in configuration file. Details: {e}")
        sys.exit(1)

    # --- Execute Scraper ---
    try:
        print(f"\nExecuting '{args.scraper_type}' scraper...")
        if args.scraper_type == "selenium":
            # The generic_selenium_scraper returns a dictionary of DataFrames
            scraped_data_dict = generic_selenium_scraper(scraper_config_json)
        # elif args.scraper_type == "requests":
        #     scraped_data_dict = generic_web_scraper(scraper_config_json) # Assumes similar return type
        else:
            print(f"Error: Scraper type '{args.scraper_type}' is not supported by this script.")
            sys.exit(1)
        
        print("\n✅ Scraper executed successfully!")

        # --- Display Results ---
        if not scraped_data_dict:
            print("Scraper returned no data.")
            return

        for target_name, df in scraped_data_dict.items():
            print(f"\n--- Results for target: '{target_name}' ---")
            if isinstance(df, pd.DataFrame):
                print(f"Shape: {df.shape[0]} rows, {df.shape[1]} columns")
                print("Columns:", df.columns.tolist())
                print("Data Head:")
                print(df.head().to_markdown(index=False))
            else:
                print(f"Warning: Expected a pandas DataFrame for target '{target_name}', but got {type(df)}.")

    except Exception as e:
        print(f"\n❌ Scraper failed with an error: {e}")
        import traceback
        print("\n--- Full Stack Trace ---")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()