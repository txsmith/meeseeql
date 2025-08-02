SELECT
    n.nspname as source_schema_name,
    t.relname as source_table_name,
    a.attname as source_column_name,
    fn.nspname as dest_schema_name,
    ft.relname as dest_table_name,
    fa.attname as dest_column_name,
    c.conname as constraint_name
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
JOIN pg_namespace n ON t.relnamespace = n.oid
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
JOIN pg_class ft ON c.confrelid = ft.oid
JOIN pg_namespace fn ON ft.relnamespace = fn.oid
JOIN pg_attribute fa ON fa.attrelid = ft.oid AND fa.attnum = ANY(c.confkey)
WHERE c.contype = 'f'
AND (
    (LOWER(t.relname) = LOWER('{{table_name}}') AND LOWER(n.nspname) = LOWER('{{schema_name}}'))
    OR
    (LOWER(ft.relname) = LOWER('{{table_name}}') AND LOWER(fn.nspname) = LOWER('{{schema_name}}'))
)
ORDER BY
    CASE WHEN LOWER(t.relname) = LOWER('{{table_name}}') THEN 0 ELSE 1 END,
    c.conname