SELECT
    c.column_name,
    c.data_type,
    c.is_nullable,
    c.column_default
FROM information_schema.columns c
WHERE c.table_schema NOT IN ('INFORMATION_SCHEMA')
AND LOWER(c.table_name) = LOWER('{{table_name}}')
AND LOWER(c.table_schema) = LOWER('{{schema_name}}')
ORDER BY c.ordinal_position