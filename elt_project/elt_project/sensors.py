# elt_project/sensors.py
import os
import fnmatch
from datetime import datetime

from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
)
from typing import List

from .assets.models import PipelineConfig


def generate_file_sensors(configs: List[PipelineConfig], jobs_by_import_name: dict) -> list:
    """
    Generates a list of file sensors from a list of pipeline configurations.

    Args:
        configs (List[PipelineConfig]): A list of all pipeline configurations.
        jobs_by_import_name (dict): A mapping from import_name to its corresponding job name.

    Returns:
        list: A list of Dagster sensor definitions.
    """
    sensors = []
    for config in configs:
        if config.monitored_directory and config.import_name in jobs_by_import_name:
            sensors.append(create_file_sensor(config, job_name=jobs_by_import_name[config.import_name]))
    return sensors

def create_file_sensor(config: PipelineConfig, job_name: str):
    """
    Factory to create a file sensor for a given pipeline configuration.

    This sensor monitors a directory for new or modified files matching a pattern.
    When a new file is detected, it yields a RunRequest for the specific job
    associated with that import, passing the file path in the run configuration.

    Args:
        config (PipelineConfig): The configuration for the pipeline import.
        job_name (str): The name of the job to trigger.

    Returns:
        A Dagster sensor definition.
    """

    @sensor(name=f"sensor_{config.import_name}", job_name=job_name, minimum_interval_seconds=30)
    def _file_sensor(context: SensorEvaluationContext):
        """
        The actual sensor function that Dagster executes.
        """
        # --- Start of Debugging ---
        context.log.info(f"Sensor '{config.import_name}' ticking.")
        context.log.info(f"-> Monitoring Directory: '{config.monitored_directory}'")
        context.log.info(f"-> For File Pattern: '{config.file_pattern}'")
        # --- End of Debugging ---

        if not config.monitored_directory or not os.path.isdir(config.monitored_directory):
            return SkipReason(f"Monitored directory not found: {config.monitored_directory}")

        # Get the last processed timestamp from the cursor
        last_mtime = float(context.cursor) if context.cursor else 0
        max_mtime = last_mtime
        context.log.info(f"-> Last processed file time (from cursor): {datetime.fromtimestamp(last_mtime).isoformat() if last_mtime > 0 else 'Never run before'}")
        
        for filename in os.listdir(config.monitored_directory):
            if fnmatch.fnmatch(filename, config.file_pattern):
                filepath = os.path.join(config.monitored_directory, filename)
                # --- Start of Debugging ---
                context.log.info(f"  - Found matching file: '{filename}'")
                # --- End of Debugging ---
                try:
                    mtime = os.path.getmtime(filepath)
                    context.log.info(f"    - File's modification time: {datetime.fromtimestamp(mtime).isoformat()}")
                    
                    # This is the core condition. A run is triggered only if the file is newer than the last one we processed.
                    if mtime > last_mtime:
                        context.log.info("    - !!! CONDITION MET: File is new. Triggering a run. !!!")
                        run_key = f"{config.import_name}:{filepath}:{mtime}"
                        
                        # This is the run_config that the asset will receive
                        run_config = {
                            "ops": {
                                f"{config.import_name}_extract_and_load_staging": {
                                    "config": {"source_file_path": filepath}
                                }
                            }
                        }
                        
                        # Yield a RunRequest for the specific job with the config.
                        yield RunRequest(run_key=run_key, run_config=run_config, job_name=job_name)
                        max_mtime = max(max_mtime, mtime)

                except FileNotFoundError:
                    # File might have been deleted between listdir and getmtime
                    context.log.warning(f"File {filepath} not found during sensor evaluation.")

        # --- Start of Debugging ---
        context.log.info(f"-> Finished checking files. New cursor value will be: {datetime.fromtimestamp(max_mtime).isoformat() if max_mtime > 0 else 'Unchanged'}")
        # --- End of Debugging ---
        if max_mtime > last_mtime:
            context.update_cursor(str(max_mtime))

    return _file_sensor