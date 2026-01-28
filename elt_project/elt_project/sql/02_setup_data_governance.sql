-- 04_setup_data_governance.sql

-- This script sets up the tables and stored procedure for the dynamic data governance framework.

-- 1. Table to store data quality rules
CREATE TABLE data_quality_rules (
    rule_id INT IDENTITY(1,1) PRIMARY KEY,
    rule_name NVARCHAR(255) NOT NULL UNIQUE,
    description NVARCHAR(1024) NULL,
    target_table NVARCHAR(255) NOT NULL, -- The staging table to check (e.g., 'stg_ri_dbt')
    target_column NVARCHAR(255) NOT NULL, -- The column to check
    check_type NVARCHAR(50) NOT NULL, -- e.g., 'NOT_NULL', 'UNIQUE', 'REGEX_MATCH', 'IS_IN_SET'
    check_expression NVARCHAR(MAX) NULL, -- The value for the check (e.g., a regex pattern, a list of values)
    is_active BIT NOT NULL DEFAULT 1,
    severity NVARCHAR(50) NOT NULL DEFAULT 'WARN', -- 'WARN' or 'FAIL'. 'FAIL' can stop the pipeline.
    created_at DATETIME DEFAULT GETUTCDATE()
);
GO

-- 2. Table to log the results of data quality checks
CREATE TABLE data_quality_run_logs (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    run_id NVARCHAR(255) NOT NULL,
    rule_id INT NOT NULL,
    rule_name NVARCHAR(255) NOT NULL,
    check_timestamp DATETIME DEFAULT GETUTCDATE(),
    status NVARCHAR(50) NOT NULL, -- 'PASS' or 'FAIL'
    failing_row_count INT NOT NULL,
    details NVARCHAR(MAX) NULL,
    FOREIGN KEY (rule_id) REFERENCES data_quality_rules(rule_id)
);
GO

-- Helper function to safely split a string into a table of values.
-- This is used to prevent SQL injection in the 'IS_IN_SET' check.
CREATE FUNCTION dbo.fn_split_string
(
    @string NVARCHAR(MAX),
    @delimiter CHAR(1)
)
RETURNS @output TABLE(splitdata NVARCHAR(MAX))
BEGIN
    DECLARE @start INT, @end INT
    SELECT @start = 1, @end = CHARINDEX(@delimiter, @string)
    WHILE @start < LEN(@string) + 1 BEGIN
        IF @end = 0
            SET @end = LEN(@string) + 1

        INSERT INTO @output (splitdata)
        VALUES(LTRIM(RTRIM(SUBSTRING(@string, @start, @end - @start))))

        SET @start = @end + 1
        SET @end = CHARINDEX(@delimiter, @string, @start)
    END
    RETURN
END
GO
-- 3. Stored procedure to execute data quality checks
CREATE OR ALTER PROCEDURE sp_execute_data_quality_checks
    @run_id NVARCHAR(255),
    @target_table NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rule_id INT, @rule_name NVARCHAR(255), @target_column NVARCHAR(255), @check_type NVARCHAR(50), @check_expression NVARCHAR(MAX);
    DECLARE @sql NVARCHAR(MAX), @params NVARCHAR(MAX), @failing_row_count INT;

    -- Cursor to iterate over all active rules for the specified target table
    DECLARE rule_cursor CURSOR FOR
    SELECT rule_id, rule_name, target_column, check_type, check_expression
    FROM data_quality_rules
    WHERE is_active = 1 AND target_table = @target_table;

    OPEN rule_cursor;
    FETCH NEXT FROM rule_cursor INTO @rule_id, @rule_name, @target_column, @check_type, @check_expression;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @failing_row_count = 0;
        SET @sql = '';
        SET @params = N'@count INT OUTPUT, @run_id NVARCHAR(255)';

        -- Dynamically build the SQL query for the check based on its type
        -- All checks are performed only on the data from the current run
        IF @check_type = 'NOT_NULL'
            SET @sql = N'SELECT @count = COUNT(*) FROM ' + QUOTENAME(@target_table) + N' WHERE ' + QUOTENAME(@target_column) + N' IS NULL AND dagster_run_id = @run_id;';
        
        ELSE IF @check_type = 'UNIQUE'
            SET @sql = N'SELECT @count = COUNT(*) FROM (SELECT 1 FROM ' + QUOTENAME(@target_table) + N' WHERE dagster_run_id = @run_id GROUP BY ' + QUOTENAME(@target_column) + N' HAVING COUNT(*) > 1) AS duplicates;';

        ELSE IF @check_type = 'REGEX_MATCH' AND @check_expression IS NOT NULL
            -- Note: SQL Server does not have a built-in REGEX function. This uses LIKE for simple patterns.
            -- The check_expression is now parameterized to prevent SQL injection.
            BEGIN
                SET @sql = N'SELECT @count = COUNT(*) FROM ' + QUOTENAME(@target_table) + N' WHERE ' + QUOTENAME(@target_column) + N' NOT LIKE @expr AND dagster_run_id = @run_id;';
                SET @params = @params + N', @expr NVARCHAR(MAX)';
            END

        ELSE IF @check_type = 'IS_IN_SET' AND @check_expression IS NOT NULL
            -- This is now safe from SQL injection by using a table-valued function to parse the list.
            BEGIN
                SET @sql = N'SELECT @count = COUNT(*) FROM ' + QUOTENAME(@target_table) + N' WHERE ' + QUOTENAME(@target_column) + N' NOT IN (SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@expr, '','')) AND dagster_run_id = @run_id;';
                SET @params = @params + N', @expr NVARCHAR(MAX)';
            END

        -- Execute the dynamically generated SQL
        IF @sql != ''
        BEGIN
            EXEC sp_executesql @sql, @params, @count = @failing_row_count OUTPUT, @run_id = @run_id, @expr = @check_expression;

            -- Log the result of the check
            INSERT INTO data_quality_run_logs (run_id, rule_id, rule_name, status, failing_row_count, details)
            VALUES (
                @run_id,
                @rule_id,
                @rule_name,
                CASE WHEN @failing_row_count = 0 THEN 'PASS' ELSE 'FAIL' END,
                @failing_row_count,
                CASE WHEN @failing_row_count > 0 THEN 'Check failed for ' + CAST(@failing_row_count AS NVARCHAR) + ' rows.' ELSE 'Check passed.' END
            );
        END

        FETCH NEXT FROM rule_cursor INTO @rule_id, @rule_name, @target_column, @check_type, @check_expression;
    END

    CLOSE rule_cursor;
    DEALLOCATE rule_cursor;

    -- Return the total number of failing rules for this run/table combination
    SELECT SUM(failing_row_count) FROM data_quality_run_logs WHERE run_id = @run_id AND rule_id IN (SELECT rule_id FROM data_quality_rules WHERE target_table = @target_table);
END;
GO