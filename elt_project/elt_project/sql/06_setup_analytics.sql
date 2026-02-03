-- 06_setup_analytics.sql

-- Table to store configuration for automated analytics (which tables to analyze)
CREATE TABLE analytics_config (
    id INT IDENTITY(1,1) PRIMARY KEY,
    target_table NVARCHAR(255) NOT NULL, -- The destination table to analyze
    date_column NVARCHAR(255) NOT NULL,  -- The time-series column
    value_column NVARCHAR(255) NOT NULL, -- The metric to forecast/analyze
    model_type NVARCHAR(50) DEFAULT 'anomaly_detection', -- 'anomaly_detection' or 'forecast'
    alert_webhook_url NVARCHAR(MAX) NULL, -- Optional: Slack/Teams webhook for alerts
    is_active BIT DEFAULT 1
);
GO

-- Table to store the output of predictive models
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
GO