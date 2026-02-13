-- 03_setup_log_table.sql

-- This table will store detailed logs and metrics for each asset run,
-- suitable for auditing, performance tracking, and business analytics.
CREATE TABLE etl_pipeline_run_logs (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    run_id NVARCHAR(255) NOT NULL, -- The unique ID for a Dagster run.
    pipeline_name NVARCHAR(255) NOT NULL,
    import_name NVARCHAR(255) NOT NULL,
    asset_name NVARCHAR(255) NOT NULL,
    status NVARCHAR(50) NOT NULL, -- e.g., 'SUCCESS', 'FAILURE'
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL,
    duration_seconds AS DATEDIFF(second, start_time, end_time), -- Computed column for duration
    rows_processed INT NULL, -- Number of rows read from source or affected by transform
    message NVARCHAR(MAX) NULL, -- For general messages or summary of error
    error_details NVARCHAR(MAX) NULL, -- Detailed stack trace or error information
    resolution_steps NVARCHAR(MAX) NULL -- Suggested steps to resolve the error
);
GO