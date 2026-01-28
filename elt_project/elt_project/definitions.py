# elt_project/definitions.py
import os
import json
from dotenv import load_dotenv
from dagster import Definitions, define_asset_job, AssetSelection
from .assets.factory import create_extract_and_load_asset, create_transform_asset, create_pipeline_setup_utility_asset, create_pipeline_column_mapping_utility_asset, create_backup_utility_asset
from .assets.models import PipelineConfig # This import is correct
from .assets.resources import SQLServerResource # This import will now work correctly
from .sensors import generate_file_sensors
from typing import Dict, Tuple, List

from sqlalchemy import text, inspect

# The SQLServerResource class has been moved to elt_project/assets/resources.py
# to resolve the import error and improve project structure.

# Load environment variables from .env file
load_dotenv()

def load_all_definitions_from_db(db_resource: SQLServerResource) -> Tuple[list, list, list]:
    """
    Connects to the database, queries the config table, and generates assets, sensors, and jobs.
    """
    all_assets = []
    all_pipeline_configs: List[PipelineConfig] = []
    all_jobs = []
    configs_by_pipeline: Dict[str, List[PipelineConfig]] = {}
    extract_assets_by_import_name: Dict[str, object] = {}
    jobs_by_import_name: Dict[str, str] = {}
    engine = db_resource.get_engine()
    
    with engine.connect() as connection:
        # --- Dynamic Column Selection for Robustness ---
        # 1. Get the expected columns from the Pydantic model.
        expected_columns = set(PipelineConfig.model_fields.keys())

        # 2. Inspect the actual columns in the database table.
        inspector = inspect(engine)
        try:
            actual_columns = {col['name'] for col in inspector.get_columns('elt_pipeline_configs')}
        except Exception as e:
            raise Exception(f"Could not inspect columns for 'elt_pipeline_configs'. Ensure the table exists. Error: {e}")

        # 3. Find the intersection of columns that exist in both the model and the table.
        columns_to_select = list(expected_columns.intersection(actual_columns))
        columns_str = ", ".join(columns_to_select)

        # 4. Build and execute the query.
        query = text(f"SELECT {columns_str} FROM elt_pipeline_configs WHERE is_active = 1")
        configs = connection.execute(query).mappings().all()

        # Step 1: Create all extract assets first, as other assets and jobs will depend on them.
        for config_row in configs:
            # Convert the SQLAlchemy Row object to a dictionary
            config_dict = dict(config_row)
            # The Pydantic model will use default values for any missing columns.
            
            # Validate the config using the Pydantic model and store it
            pipeline_config = PipelineConfig(**config_dict)
            all_pipeline_configs.append(pipeline_config)

            pipeline_name = pipeline_config.pipeline_name.strip().lower()
            if pipeline_name not in configs_by_pipeline:
                configs_by_pipeline[pipeline_name] = []
            configs_by_pipeline[pipeline_name].append(pipeline_config)
            
            # Create the extract and load asset for this specific config
            extract_asset = create_extract_and_load_asset(pipeline_config)
            all_assets.append(extract_asset)
            extract_assets_by_import_name[pipeline_config.import_name] = extract_asset

    # Step 2: Create transform assets for each import.
    for pipeline_config in all_pipeline_configs:
        transform_asset = create_transform_asset(pipeline_config)
        all_assets.append(transform_asset)

    # Create utility assets grouped by pipeline.
    for pipeline_name, configs in configs_by_pipeline.items():
        # Create the new consolidated setup utility asset for the entire pipeline
        pipeline_setup_asset = create_pipeline_setup_utility_asset(pipeline_name, configs)
        all_assets.append(pipeline_setup_asset)

        # Create the new consolidated column mapping utility asset for the entire pipeline
        pipeline_mapping_asset = create_pipeline_column_mapping_utility_asset(pipeline_name, configs)
        all_assets.append(pipeline_mapping_asset)

    # Create the single, standalone backup utility asset
    backup_asset = create_backup_utility_asset()
    all_assets.append(backup_asset)

    # Step 3: Now that all assets are defined, create the jobs.
    # Create a job for each individual import that includes its downstream transform.
    # This is what the sensor will trigger.
    for pipeline_config in all_pipeline_configs:
        extract_asset = extract_assets_by_import_name[pipeline_config.import_name]
        # The selection includes the extract asset and all its downstream dependencies (the transform asset).
        # This ensures that when a sensor triggers this job, both staging and transform steps run.
        single_import_job = define_asset_job(
            name=f"{pipeline_config.import_name}_job",
            selection=AssetSelection.assets(extract_asset).downstream(),
            tags={"dagster/concurrency_key": f"lock_{pipeline_config.pipeline_name.strip().lower()}"}
        )
        all_jobs.append(single_import_job)
        jobs_by_import_name[pipeline_config.import_name] = single_import_job.name

    # Step 4: Generate sensors that trigger these new, complete jobs.
    all_sensors = generate_file_sensors(all_pipeline_configs, jobs_by_import_name, db_resource)

    # Step 5: Create jobs for entire asset groups (for manual runs in the UI).
    group_jobs = [
        define_asset_job(
            name=job_name, 
            selection=AssetSelection.groups(job_name),
            tags={"dagster/concurrency_key": f"lock_{job_name}"}
        ) 
        for job_name in configs_by_pipeline.keys()
    ]
    all_jobs.extend(group_jobs)

    # Add a dedicated job for all utility assets
    utility_job = define_asset_job(name="utility_jobs", selection=AssetSelection.groups("_utility"))
    all_jobs.append(utility_job)

    return all_assets, all_sensors, all_jobs

# Instantiate the resource to be used for loading configs and by assets
sql_server_resource = SQLServerResource(
    driver=os.getenv("DB_DRIVER", "").strip("{}"), # Remove braces if present
    server=os.getenv("DB_SERVER"),
    database=os.getenv("DB_DATABASE"),
    username=os.getenv("DB_USERNAME"),
    password=os.getenv("DB_PASSWORD"),
    credential_target=os.getenv("CREDENTIAL_TARGET"),
    trust_server_certificate=os.getenv("DB_TRUST_SERVER_CERTIFICATE", "no"),
)

# Load all assets, sensors, and jobs from the database
all_assets, all_sensors, all_jobs = load_all_definitions_from_db(sql_server_resource)

# The Definitions object is what Dagster loads
defs = Definitions(
    assets=all_assets,
    sensors=all_sensors, # Add the generated sensors here
    jobs=all_jobs, # Add the explicitly defined jobs here
    resources={"db_resource": sql_server_resource}
)
