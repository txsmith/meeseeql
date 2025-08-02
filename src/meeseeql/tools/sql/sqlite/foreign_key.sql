SELECT
    'main' as source_schema_name,
    m.name as source_table_name,
    fk."from" as source_column_name,
    'main' as dest_schema_name,
    fk."table" as dest_table_name,
    fk."to" as dest_column_name,
    'fk_' || m.name || '_' || fk.id as constraint_name
FROM sqlite_master m
JOIN pragma_foreign_key_list(m.name) fk
WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%'
AND (LOWER(m.name) = LOWER('{{table_name}}') OR LOWER(fk."table") = LOWER('{{table_name}}'))
ORDER BY
    CASE WHEN LOWER(m.name) = LOWER('{{table_name}}') THEN 0 ELSE 1 END,
    constraint_name