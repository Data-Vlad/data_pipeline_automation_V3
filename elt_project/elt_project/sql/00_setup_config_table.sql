-- 00_setup_config_table.sql

-- This table will store the metadata for all ELT pipelines,
-- replacing the need for individual YAML files.
CREATE TABLE elt_pipeline_configs (
    id INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name NVARCHAR(255) NOT NULL, -- The logical pipeline group/name this source belongs to. E.g., 'ri_pipeline'.
    import_name NVARCHAR(255) NOT NULL UNIQUE, -- The unique name for the data source, used for asset naming. E.g., 'ri_dbt'.
    file_pattern NVARCHAR(512) NOT NULL, -- Pattern or name of the source file to look for. E.g., 'RI_DBT.csv', 'sales_*.csv'.
    monitored_directory NVARCHAR(512) NULL, -- The directory Dagster should monitor for the file_pattern. If NULL, no file monitoring.
    connection_string NVARCHAR(1024) NULL, -- Optional connection string for a pipeline-specific database. If NULL, the default resource is used.
    file_type NVARCHAR(50) NOT NULL, -- The type of parser to use, must match a key in `core/parsers.py`. E.g., 'csv', 'psv'.
    staging_table NVARCHAR(255) NOT NULL, -- The name of the SQL staging table to load raw data into. E.g., 'stg_products'.
    column_mapping NVARCHAR(MAX) NULL, -- Optional JSON string for mapping source columns to staging table columns. E.g., '{"SourceCol": "StagingCol"}'.
    destination_table NVARCHAR(255) NOT NULL, -- The name of the final destination table. E.g., 'dest_products'.
    parser_function NVARCHAR(255) NULL, -- Optional name of a custom parser function in core/custom_parsers.py
    transform_procedure NVARCHAR(255) NOT NULL, -- The name of the stored procedure for the transformation step. E.g., 'sp_transform_products'.
    load_method NVARCHAR(50) NOT NULL DEFAULT 'replace', -- The method for loading data into the staging table: 'replace' or 'append'.
    deduplication_key NVARCHAR(512) NULL, -- Optional comma-separated list of columns to use for deduplication on 'append'. E.g., 'ID,Date'.
    created_at DATETIME DEFAULT GETUTCDATE(), -- Timestamp for when the configuration was added.
    is_active BIT NOT NULL DEFAULT 1, -- A flag to easily enable or disable a pipeline from running without deleting the config.
    last_import_date DATETIME NULL, -- Timestamp for when the pipeline was last successfully run.
);
GO
