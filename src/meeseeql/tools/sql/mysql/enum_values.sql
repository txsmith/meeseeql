SELECT
    c.column_name,
    CASE
        WHEN c.data_type = 'enum' THEN
            REPLACE(REPLACE(SUBSTRING(c.column_type, 6, CHAR_LENGTH(c.column_type) - 6), '''', ''), ')', '')
        ELSE NULL
    END as enum_values
FROM information_schema.columns c
WHERE c.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
AND LOWER(c.table_name) = LOWER('{{table_name}}')
AND LOWER(c.table_schema) = LOWER('{{schema_name}}')
AND c.data_type = 'enum'
ORDER BY c.ordinal_position