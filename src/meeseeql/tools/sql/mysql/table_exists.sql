SELECT table_name FROM information_schema.tables
WHERE LOWER(table_name) = LOWER('{{table_name}}')
AND table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
AND LOWER(table_schema) = LOWER('{{schema_name}}')
LIMIT 1