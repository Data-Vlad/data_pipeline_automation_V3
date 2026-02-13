# elt_project/sensors.py
import os
import fnmatch
from datetime import datetime
import re
import json
from sqlalchemy import text

from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
)
from typing import List

from .assets.models import PipelineConfig
from .assets.resources import SQLServerResource

def sanitize_name(name: str) -> str:
    """
    Sanitizes a string to be a valid Dagster name by replacing
    all non-alphanumeric characters with underscores.
    """
    return re.sub(r'[^A-Za-z0-9_]', '_', name)

def generate_file_sensors(configs: List[PipelineConfig], jobs_by_import_name: dict, db_resource: SQLServerResource) -> list:
    """
    Generates a list of file sensors from a list of pipeline configurations.

    Args:
        configs (List[PipelineConfig]): A list of all pipeline configurations.
        jobs_by_import_name (dict): A mapping from import_name to its corresponding job name.
        db_resource (SQLServerResource): Database resource for runtime config checks.

    Returns:
        list: A list of Dagster sensor definitions.
    """
    sensors = []
    for config in configs:
        if config.monitored_directory and config.import_name in jobs_by_import_name:
            sensors.append(create_file_sensor(config, job_name=jobs_by_import_name[config.import_name], db_resource=db_resource))
    return sensors

def create_file_sensor(config: PipelineConfig, job_name: str, db_resource: SQLServerResource):
    """
    Factory to create a file sensor for a given pipeline configuration.

    This sensor monitors a directory for new or modified files matching a pattern.
    When a new file is detected, it yields a RunRequest for the specific job
    associated with that import, passing the file path in the run configuration.

    Args:
        config (PipelineConfig): The configuration for the pipeline import.
        job_name (str): The name of the job to trigger.
        db_resource (SQLServerResource): Database resource for runtime config checks.

    Returns:
        A Dagster sensor definition.
    """
    # Ensure the sensor name is valid and unique per import
    sensor_name = sanitize_name(f"sensor_{config.import_name}")

    # Sanitize the file pattern to ensure it only contains the filename/wildcard, not a directory path.
    target_file_pattern = os.path.basename(config.file_pattern) if config.file_pattern else "*"

    @sensor(name=sensor_name, job_name=job_name, minimum_interval_seconds=30)
    def _file_sensor(context: SensorEvaluationContext):
        """
        The actual sensor function that Dagster executes.
        """
        # --- Runtime Config Check ---
        # We fetch the latest config from the DB to see if it matches what was loaded at startup.
        current_staging_display = config.staging_table
        current_mode_display = config.load_method.upper()
        restart_required = False
        db_staging_table = None

        try:
            engine = db_resource.get_engine()
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT staging_table, load_method, scraper_config FROM elt_pipeline_configs WHERE import_name = :import_name"),
                    {"import_name": config.import_name}
                ).mappings().one_or_none()
                
                if row:
                    db_staging_table = row['staging_table']
                    # Check for dependency override in DB config
                    if row['scraper_config']:
                        try:
                            sc_config = json.loads(row['scraper_config'])
                            if isinstance(sc_config, dict) and "depends_on" in sc_config:
                                current_mode_display = "APPEND (Override: Dependency)"
                        except Exception:
                            pass
                    
                    # Detect mismatch
                    if db_staging_table != config.staging_table:
                        current_staging_display = f"{config.staging_table} (DB: {db_staging_table})"
                        restart_required = True
        except Exception as e:
            print(f"  > Warning: Could not verify DB config: {e}")
        
        # --- Simplified Console Output ---
        print(f"[SENSOR] Checking '{config.import_name}' (Stg: {current_staging_display} | Mode: {current_mode_display})...")
        
        if restart_required:
            print(f"  > [!] CONFIG MISMATCH: Database staging table '{db_staging_table}' differs from loaded config.")
            print(f"  > [!] ACTION REQUIRED: You must restart Dagster (close and re-run) to apply this change.")

        if not config.monitored_directory or not os.path.isdir(config.monitored_directory):
            print(f"  > ERROR: Directory '{config.monitored_directory}' not found.")
            context.log.error(f"Directory not found: {config.monitored_directory}")
            return SkipReason(f"Monitored directory not found: {config.monitored_directory}")

        # Get the last processed timestamp from the cursor
        last_mtime = float(context.cursor) if context.cursor else 0
        max_mtime = last_mtime

        try:
            matching_files = [f for f in os.listdir(config.monitored_directory) if fnmatch.fnmatch(f, target_file_pattern)]
        except Exception as e:
            print(f"  > ERROR reading directory: {e}")
            return

        if not matching_files:
            print(f"  > No files match '{target_file_pattern}' in '{config.monitored_directory}'.")
            return

        new_files_count = 0
        for filename in matching_files:
            filepath = os.path.join(config.monitored_directory, filename)
            try:
                mtime = os.path.getmtime(filepath)
                
                if mtime > last_mtime:
                    print(f"  > [+] NEW FILE: {filename} -> Triggering Run (Staging: {config.staging_table} -> Dest: {config.destination_table})")
                    context.log.info(f"Triggering run for new file: {filename}")
                    
                    run_key = f"{config.import_name}:{filepath}:{mtime}"
                    
                    run_config = {
                        "ops": {
                            f"{config.import_name}_extract_and_load_staging": {
                                "config": {"source_file_path": filepath}
                            }
                        }
                    }
                    
                    yield RunRequest(run_key=run_key, run_config=run_config, job_name=job_name)
                    max_mtime = max(max_mtime, mtime)
                    new_files_count += 1

            except FileNotFoundError:
                pass

        if new_files_count == 0:
            print(f"  > Checked {len(matching_files)} files. No new data.")

        if max_mtime > last_mtime:
            context.update_cursor(str(max_mtime))

    return _file_sensor