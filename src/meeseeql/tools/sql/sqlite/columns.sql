SELECT
    p.name as column_name,
    p.type as data_type,
    CASE WHEN p."notnull" = 0 THEN 'YES' ELSE 'NO' END as is_nullable,
    p.dflt_value as column_default
FROM pragma_table_info('{{table_name}}') p
ORDER BY p.cid