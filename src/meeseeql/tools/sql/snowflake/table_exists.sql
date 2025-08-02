SELECT table_name FROM information_schema.tables
WHERE LOWER(table_name) = LOWER('{{table_name}}')
AND table_schema NOT IN ('INFORMATION_SCHEMA')
AND LOWER(table_schema) = LOWER('{{schema_name}}')
LIMIT 1