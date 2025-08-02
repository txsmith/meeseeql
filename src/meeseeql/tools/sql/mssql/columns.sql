SELECT
    c.name as column_name,
    tp.name as data_type,
    CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END as is_nullable,
    dc.definition as column_default
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.columns c ON t.object_id = c.object_id
INNER JOIN sys.types tp ON c.user_type_id = tp.user_type_id
LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
WHERE s.name NOT IN ('information_schema', 'sys')
AND LOWER(t.name) = LOWER('{{table_name}}')
AND LOWER(s.name) = LOWER('{{schema_name}}')
ORDER BY c.column_id