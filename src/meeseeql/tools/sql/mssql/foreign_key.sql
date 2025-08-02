SELECT
    s.name as source_schema_name,
    t.name as source_table_name,
    c.name as source_column_name,
    rs.name as dest_schema_name,
    rt.name as dest_table_name,
    rc.name as dest_column_name,
    fk.name as constraint_name
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
JOIN sys.tables t ON fk.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
WHERE s.name NOT IN ('information_schema', 'sys')
AND (
    (LOWER(t.name) = LOWER('{{table_name}}') AND LOWER(s.name) = LOWER('{{schema_name}}'))
    OR
    (LOWER(rt.name) = LOWER('{{table_name}}') AND LOWER(rs.name) = LOWER('{{schema_name}}'))
)
ORDER BY
    CASE WHEN LOWER(t.name) = LOWER('{{table_name}}') THEN 0 ELSE 1 END,
    fk.name