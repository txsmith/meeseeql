SELECT
    c.name as column_name
FROM sys.key_constraints kc
JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
JOIN sys.tables t ON kc.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE kc.type = 'PK'
AND LOWER(t.name) = LOWER('{{table_name}}')
AND LOWER(s.name) = LOWER('{{schema_name}}')
ORDER BY ic.key_ordinal