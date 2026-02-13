-- 1. Create Data Quality Rules Table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[data_quality_rules]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[data_quality_rules](
        [rule_id] [int] IDENTITY(1,1) NOT NULL,
        [rule_name] [nvarchar](255) NOT NULL,
        [description] [nvarchar](max) NULL,
        [target_table] [nvarchar](255) NOT NULL,
        [target_column] [nvarchar](255) NULL,
        [check_type] [nvarchar](50) NOT NULL, -- 'NOT_NULL', 'UNIQUE', 'IS_IN_SET', 'CUSTOM_SQL'
        [check_expression] [nvarchar](max) NULL,
        [severity] [nvarchar](50) NOT NULL DEFAULT 'FAIL', -- 'FAIL', 'WARN'
        [is_active] [bit] NOT NULL DEFAULT 1,
        [created_at] [datetime] DEFAULT GETUTCDATE(),
        PRIMARY KEY CLUSTERED ([rule_id] ASC)
    );
END
GO

-- 2. Create Data Quality Run Logs Table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[data_quality_run_logs]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[data_quality_run_logs](
        [log_id] [int] IDENTITY(1,1) NOT NULL,
        [run_id] [nvarchar](255) NOT NULL,
        [rule_id] [int] NOT NULL,
        [status] [nvarchar](50) NOT NULL, -- 'PASS', 'FAIL', 'ERROR'
        [rows_failed] [int] DEFAULT 0,
        [executed_at] [datetime] DEFAULT GETUTCDATE(),
        [error_message] [nvarchar](max) NULL,
        PRIMARY KEY CLUSTERED ([log_id] ASC)
    );
END
GO

-- 3. Create Stored Procedure to Execute Checks
CREATE OR ALTER PROCEDURE [dbo].[sp_execute_data_quality_checks]
    @run_id NVARCHAR(255),
    @target_table NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    -- If no active rules exist for this table, return 0 failures immediately
    IF NOT EXISTS (SELECT 1 FROM data_quality_rules WHERE target_table = @target_table AND is_active = 1)
    BEGIN
        SELECT 0;
        RETURN;
    END

    DECLARE @rule_id INT;
    DECLARE @check_type NVARCHAR(50);
    DECLARE @target_column NVARCHAR(255);
    DECLARE @check_expression NVARCHAR(MAX);
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @failed_count INT;
    DECLARE @error_msg NVARCHAR(MAX);

    -- Iterate through active rules for the target table
    DECLARE rule_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT rule_id, check_type, target_column, check_expression
    FROM data_quality_rules
    WHERE target_table = @target_table AND is_active = 1;

    OPEN rule_cursor;
    FETCH NEXT FROM rule_cursor INTO @rule_id, @check_type, @target_column, @check_expression;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @failed_count = 0;
        SET @sql = '';
        SET @error_msg = NULL;

        BEGIN TRY
            -- Construct validation SQL based on check type
            IF @check_type = 'NOT_NULL'
            BEGIN
                SET @sql = N'SELECT @cnt = COUNT(*) FROM ' + QUOTENAME(@target_table) + 
                           N' WHERE dagster_run_id = @rid AND ' + QUOTENAME(@target_column) + N' IS NULL';
            END
            ELSE IF @check_type = 'UNIQUE'
            BEGIN
                SET @sql = N'SELECT @cnt = COUNT(*) FROM (SELECT ' + QUOTENAME(@target_column) + 
                           N' FROM ' + QUOTENAME(@target_table) + N' WHERE dagster_run_id = @rid GROUP BY ' + QUOTENAME(@target_column) + 
                           N' HAVING COUNT(*) > 1) as dupes';
            END
            ELSE IF @check_type = 'IS_IN_SET'
            BEGIN
                -- check_expression example: "'A','B','C'"
                SET @sql = N'SELECT @cnt = COUNT(*) FROM ' + QUOTENAME(@target_table) + 
                           N' WHERE dagster_run_id = @rid AND ' + QUOTENAME(@target_column) + N' NOT IN (' + @check_expression + N')';
            END
             ELSE IF @check_type = 'CUSTOM_SQL'
            BEGIN
                -- check_expression example: "Amount < 0"
                SET @sql = N'SELECT @cnt = COUNT(*) FROM ' + QUOTENAME(@target_table) + 
                           N' WHERE dagster_run_id = @rid AND (' + @check_expression + N')';
            END

            -- Execute the check if SQL was generated
            IF @sql <> ''
            BEGIN
                EXEC sp_executesql @sql, N'@rid NVARCHAR(255), @cnt INT OUTPUT', @rid = @run_id, @cnt = @failed_count OUTPUT;
            END

            -- Log result
            INSERT INTO data_quality_run_logs (run_id, rule_id, status, rows_failed, executed_at)
            VALUES (@run_id, @rule_id, CASE WHEN @failed_count > 0 THEN 'FAIL' ELSE 'PASS' END, @failed_count, GETUTCDATE());

        END TRY
        BEGIN CATCH
            -- Log error execution
            SET @error_msg = ERROR_MESSAGE();
            INSERT INTO data_quality_run_logs (run_id, rule_id, status, error_message, executed_at)
            VALUES (@run_id, @rule_id, 'ERROR', @error_msg, GETUTCDATE());
        END CATCH

        FETCH NEXT FROM rule_cursor INTO @rule_id, @check_type, @target_column, @check_expression;
    END

    CLOSE rule_cursor;
    DEALLOCATE rule_cursor;

    -- Return total failing rows (FAIL severity) for immediate feedback to the pipeline
    SELECT ISNULL(SUM(rows_failed), 0) 
    FROM data_quality_run_logs l
    JOIN data_quality_rules r ON l.rule_id = r.rule_id
    WHERE l.run_id = @run_id AND l.status = 'FAIL' AND r.severity = 'FAIL';
END;
GO
