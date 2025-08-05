SELECT
    c.column_name,
    string_agg(e.enumlabel, ', ' ORDER BY e.enumsortorder) as enum_values
FROM information_schema.columns c
JOIN pg_type t ON t.typname = c.udt_name
JOIN pg_enum e ON e.enumtypid = t.oid
WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog')
AND LOWER(c.table_name) = LOWER('{{table_name}}')
AND LOWER(c.table_schema) = LOWER('{{schema_name}}')
AND t.typtype = 'e'
GROUP BY c.column_name, c.ordinal_position
ORDER BY c.ordinal_position