SELECT
    kcu.table_schema as source_schema_name,
    kcu.table_name as source_table_name,
    kcu.column_name as source_column_name,
    kcu.referenced_table_schema as dest_schema_name,
    kcu.referenced_table_name as dest_table_name,
    kcu.referenced_column_name as dest_column_name,
    kcu.constraint_name
FROM information_schema.key_column_usage kcu
WHERE kcu.referenced_table_name IS NOT NULL
AND kcu.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
AND (
    (LOWER(kcu.table_name) = LOWER('{{table_name}}') AND LOWER(kcu.table_schema) = LOWER('{{schema_name}}'))
    OR
    (LOWER(kcu.referenced_table_name) = LOWER('{{table_name}}') AND LOWER(kcu.referenced_table_schema) = LOWER('{{schema_name}}'))
)
ORDER BY
    CASE WHEN LOWER(kcu.table_name) = LOWER('{{table_name}}') THEN 0 ELSE 1 END,
    kcu.constraint_name