-- 1. Staging Table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[stg_video_assets]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[stg_video_assets](
        [file_name] [nvarchar](255) NULL,
        [file_path] [nvarchar](max) NULL,
        [file_size_bytes] [bigint] NULL,
        [duration_seconds] [float] NULL,
        [fps] [float] NULL,
        [width] [int] NULL,
        [height] [int] NULL,
        [aspect_ratio] [float] NULL,
        [processed_at] [datetime] NULL,
        [dagster_run_id] [nvarchar](255) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO

-- 2. Destination Table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[dest_video_assets]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[dest_video_assets](
        [video_id] [int] IDENTITY(1,1) NOT NULL,
        [file_name] [nvarchar](255) NULL,
        [file_path] [nvarchar](max) NULL,
        [file_size_bytes] [bigint] NULL,
        [duration_seconds] [float] NULL,
        [fps] [float] NULL,
        [width] [int] NULL,
        [height] [int] NULL,
        [aspect_ratio] [float] NULL,
        [processed_at] [datetime] NULL,
        [dagster_run_id] [nvarchar](255) NULL,
        [loaded_at] [datetime] DEFAULT (getutcdate()),
        PRIMARY KEY CLUSTERED ([video_id] ASC)
    )
END
GO

-- 3. Transformation Procedure
CREATE OR ALTER PROCEDURE [dbo].[sp_transform_video_assets]
    @run_id NVARCHAR(255),
    @tables_to_truncate NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Insert new data
    INSERT INTO dest_video_assets (
        file_name, file_path, file_size_bytes, duration_seconds,
        fps, width, height, aspect_ratio, processed_at, dagster_run_id
    )
    SELECT
        file_name, file_path, file_size_bytes, duration_seconds,
        fps, width, height, aspect_ratio, processed_at, dagster_run_id
    FROM stg_video_assets
    WHERE dagster_run_id = @run_id;
END
GO