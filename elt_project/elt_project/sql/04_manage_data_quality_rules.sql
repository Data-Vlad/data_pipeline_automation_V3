-- ====================================================================================
-- SQL Management Templates for `data_quality_rules`
--
-- Use these templates to add, update, or remove data quality rules.
-- ====================================================================================


-- ====================================================================================
-- TEMPLATE 1: INSERT a new data quality rule
--
-- Instructions:
-- 1. Uncomment the INSERT statement below.
-- 2. Fill in the values for your new rule.
-- 3. See the examples for different `check_type` values.
-- 4. Execute the script.
-- ====================================================================================

/*
INSERT INTO data_quality_rules (
    rule_name,          -- A unique, descriptive name for your rule (e.g., 'stg_users_email_not_null'). MUST BE UNIQUE.
    description,        -- (Optional) A human-readable sentence explaining what the rule does.
    target_table,       -- The staging table to check (e.g., 'stg_users').
    target_column,      -- The column to check (e.g., 'email_address').
    check_type,         -- The type of check: 'NOT_NULL', 'UNIQUE', 'REGEX_MATCH', 'IS_IN_SET'.
    check_expression,   -- (Optional) The value for the check. See examples below.
    is_active,          -- 1 for active, 0 for disabled.
    severity            -- 'WARN' (log failure and continue) or 'FAIL' (stop the pipeline on failure).
) VALUES (
    'stg_users_email_not_null',
    'Ensures the email address in the user staging table is never null.',
    'stg_users',
    'email_address',
    'NOT_NULL',
    NULL, -- No check_expression needed for NOT_NULL
    1,
    'FAIL' -- Stop the pipeline if an email is null
);
*/

-- --- EXAMPLES for different check_type values ---

-- 'UNIQUE': Checks if all values in the column are unique within the batch.
-- (rule_name, target_table, target_column, check_type, severity)
-- VALUES ('stg_products_sku_unique', 'stg_products', 'SKU', 'UNIQUE', 'FAIL');

-- 'IS_IN_SET': Checks if the column value is within a specified list.
-- The check_expression is a comma-separated string of allowed values.
-- (rule_name, target_table, target_column, check_type, check_expression, severity)
-- VALUES ('stg_orders_status_in_set', 'stg_orders', 'status', 'IS_IN_SET', '''Shipped'',''Processing'',''Cancelled''', 'WARN');

-- 'REGEX_MATCH': Checks if the column value matches a LIKE pattern.
-- The check_expression is the pattern (e.g., '%@%.%' for a basic email check).
-- (rule_name, target_table, target_column, check_type, check_expression, severity)
-- VALUES ('stg_users_email_format', 'stg_users', 'email_address', 'REGEX_MATCH', '%@%.%', 'WARN');


-- ====================================================================================
-- TEMPLATE 2: UPDATE an existing data quality rule
--
-- Instructions:
-- 1. Uncomment the UPDATE statement below.
-- 2. Set the new value for the field you want to change.
-- 3. Use the WHERE clause to target the correct rule by its `rule_name`.
-- ====================================================================================

/*
UPDATE data_quality_rules
SET
    severity = 'WARN',  -- Example: Change severity from FAIL to WARN
    is_active = 0       -- Example: Disable the rule
WHERE
    rule_name = 'stg_users_email_not_null'; -- Specify which rule to update
*/

-- ====================================================================================
-- TEMPLATE 3: DELETE a data quality rule
-- ====================================================================================

/*
DELETE FROM data_quality_rules
WHERE rule_name = 'stg_users_email_not_null'; -- Specify which rule to delete
*/